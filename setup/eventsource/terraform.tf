variable "project" {
  description = "Project name"
  type = "string"
  default = "zamus"
}

variable "stage" {
  description = "Stage"
  type = "string"
  default = "dev"
}

variable "destroy_bucket" {
  description = "Destroy Event Source Bucket"
  default = false
}

variable "sentry_dsn" {
  description = "Sentry DSN"
  type = "string"
  default = ""
}

variable "region" {
  description = "AWS Region"
  type = "string"
  default = "ap-southeast-1"
}

variable "glue_database" {
  description = "Glue Database Name"
  type = "string"
  default = "eventsource"
}

resource "aws_iam_role" "eventsource_transform_role" {
  name = "${var.project}-eventsource-transform-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

data "aws_iam_policy_document" "eventsource_transform_policies_doc" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:*:*:*"
    ]
  }
}

resource "aws_cloudwatch_log_group" "eventsource_transform_log" {
  name = "/aws/lambda/${aws_lambda_function.eventsource_transform_lambda.function_name}"
  retention_in_days = 30
}

resource "aws_iam_role_policy" "eventsource_policies" {
  role = "${aws_iam_role.eventsource_transform_role.id}"
  name = "${var.project}-eventsounce-transform-policy"
  policy = "${data.aws_iam_policy_document.eventsource_transform_policies_doc.json}"
}

resource "aws_lambda_function" "eventsource_transform_lambda" {
  filename = "eventsource.zip"
  source_code_hash = "${filebase64sha256("eventsource.zip")}"
  function_name = "${var.project}-eventsource-transform-${var.stage}"
  handler = "app"
  role = "${aws_iam_role.eventsource_transform_role.arn}"
  runtime = "go1.x"
  memory_size = 1024
  timeout = 120

  tags {
    app = "${var.project}"
    service = "eventsource-transform"
    stage = "${var.stage}"
  }

  environment {
    variables = {
      APP_STAGE = "${var.stage}"
      APP_SENTRY_DSN = "${var.sentry_dsn}"
    }
  }

}

resource "aws_sns_topic" "eventsource_sns" {
  name = "${var.project}-${var.stage}"
}

resource "aws_s3_bucket" "eventsource-bucket" {
  bucket = "${var.project}-${var.stage}"
  force_destroy = "${var.destroy_bucket}"
}

data "aws_iam_policy_document" "firehose_to_s3_role_doc" {
  statement {
    actions = [
      "sts:AssumeRole"
    ]
    effect = "Allow"

    principals {
      type = "Service"
      identifiers = [
        "firehose.amazonaws.com"
      ]
    }
  }
}

data "aws_iam_policy_document" "firehose_to_s3_policies_doc" {
  statement {
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      "*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction",
      "lambda:GetFunctionConfiguration"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "logs:PutLogEvents"
    ]
    resources = [
      "*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "glue:GetTableVersions"
    ]
    resources = [
      "*"
    ]
  }
}

resource "aws_iam_role" "firehose_to_s3_role" {
  name = "${var.project}-firehose-to-s3-role"
  assume_role_policy = "${data.aws_iam_policy_document.firehose_to_s3_role_doc.json}"
}

resource "aws_iam_role_policy" "firehose_to_s3_policies" {
  role = "${aws_iam_role.firehose_to_s3_role.id}"
  name = "${var.project}-firehose-to-s3-policy"
  policy = "${data.aws_iam_policy_document.firehose_to_s3_policies_doc.json}"
}

resource "aws_kinesis_stream" "eventsource_kinesis" {
  name = "${var.project}-${var.stage}"
  shard_count = 1
}

resource "aws_glue_catalog_database" "eventsource_glue" {
  name = "${var.glue_database}_${var.stage}"
}

resource "aws_glue_catalog_table" "eventsource_glue_table" {
  database_name = "${aws_glue_catalog_database.eventsource_glue.name}"
  name = "events"
  table_type = "EXTERNAL_TABLE"

  partition_keys {
    name = "dt"
    type = "string"
  }

  parameters {
    EXTERNAL = "TRUE"
    parquet.compression = "SNAPPY"
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.eventsource-bucket.bucket}/firehose/"
    input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    columns = [
      {
        name = "id"
        type = "string"
      },
      {
        name = "aggid"
        type = "string"
      },
      {
        name = "eventtype"
        type = "string"
      },
      {
        name = "event"
        type = "string"
      },
      {
        name = "time"
        type = "bigint"
      },
      {
        name = "seq"
        type = "bigint"
      },
      {
        name = "metadata"
        type = "map<string,string>"
      },
    ]

    ser_de_info {
      name = "eventsource"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters {
        serialization.format = 1
      }
    }
  }
}

resource "aws_kinesis_firehose_delivery_stream" "eventsource_firehose" {
  destination = "extended_s3"
  name = "${var.project}-${var.stage}"
  kinesis_source_configuration {
    kinesis_stream_arn = "${aws_kinesis_stream.eventsource_kinesis.arn}"
    role_arn = "${aws_iam_role.firehose_to_s3_role.arn}"
  }

  extended_s3_configuration {
    bucket_arn = "${aws_s3_bucket.eventsource-bucket.arn}"
    role_arn = "${aws_iam_role.firehose_to_s3_role.arn}"
    prefix = "firehose/dt=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"
    compression_format = "UNCOMPRESSED"
    buffer_interval = 900
    buffer_size = 128
    processing_configuration {
      enabled = "true"
      processors {
        type = "Lambda"
        parameters {
          parameter_name = "LambdaArn"
          parameter_value = "${aws_lambda_function.eventsource_transform_lambda.arn}:$LATEST"
        }
      }
    }
    data_format_conversion_configuration {
      enabled = true
      input_format_configuration {
        "deserializer" {
          hive_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {
            compression = "SNAPPY"
          }
        }
      }
      schema_configuration {
        database_name = "${aws_glue_catalog_table.eventsource_glue_table.database_name}"
        role_arn = "${aws_iam_role.firehose_to_s3_role.arn}"
        table_name = "${aws_glue_catalog_table.eventsource_glue_table.name}"
        region = "${var.region}"
      }
    }
  }
}

output "eventsource_sns" {
  description = "Event Source SNS"
  value = "${aws_sns_topic.eventsource_sns.0.id}"
}

output "eventsource_sns_arn" {
  description = "Event Source SNS Arn"
  value = "${aws_sns_topic.eventsource_sns.0.arn}"
}

output "eventsource_kinesis" {
  description = "Event Source Kinesis"
  value = "${aws_kinesis_stream.eventsource_kinesis.0.name}"
}

output "eventsource_kinesis_arn" {
  description = "Event Source Kinesis Arn"
  value = "${aws_kinesis_stream.eventsource_kinesis.0.arn}"
}

output "eventsource_glue_db" {
  description = "Event Source Glue DB Name"
  value = "${aws_glue_catalog_table.eventsource_glue_table.database_name}"
}

output "eventsource_glue_table" {
  description = "Event Source Glue Table Name"
  value = "${aws_glue_catalog_table.eventsource_glue_table.name}"
}
