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

variable "sentry_dsn" {
  description = "Sentry DSN"
  type = "string"
  default = ""
}

variable "tracing" {
  description = "Enable DLQ X-Ray"
  default = true
}

resource "aws_dynamodb_table" "dlq_db" {
  name = "${var.project}-dql-${var.stage}"
  hash_key = "hk"
  range_key = "rk"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "hk"
    type = "S"
  }
  attribute {
    name = "rk"
    type = "S"
  }
}

resource "aws_iam_role" "dlq_role" {
  name = "${var.project}-dlq-role"
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

data "aws_iam_policy_document" "dlq_policies_doc" {
  statement = [
    {
      effect = "Allow"
      actions = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      resources = [
        "arn:aws:logs:*:*:*"
      ]
    },
    {
      effect = "Allow"
      actions = [
        "lambda:*"
      ]
      resources = [
        "*"
      ]
    },
    {
      effect = "Allow"
      actions = [
        "dynamodb:*"
      ]
      resources = [
        "*"
      ]
    },
    {
      effect = "Allow"
      actions = [
        "xray:*"
      ]
      resources = [
        "*"
      ]
    },
  ]
}

resource "aws_iam_role_policy" "dlq_policies" {
  role = "${aws_iam_role.dlq_role.id}"
  name = "${var.project}-dlq-policy"
  policy = "${data.aws_iam_policy_document.dlq_policies_doc.json}"
}

resource "aws_lambda_function" "dlq_lambda" {
  filename = "dlq.zip"
  source_code_hash = "${filebase64sha256("dlq.zip")}"
  function_name = "${var.project}-dlq-${var.stage}"
  handler = "app"
  role = "${aws_iam_role.dlq_role.arn}"
  runtime = "go1.x"
  memory_size = 1024
  timeout = 60
  tracing_config {
    mode = "${var.tracing ? "Active" : "PassThrough"}"
  }

  tags {
    app = "${var.project}"
    service = "dlq"
    stage = "${var.stage}"
  }

  environment {
    variables = {
      APP_STAGE = "${var.stage}"
      APP_VERSION = "1.0.0"
      APP_SENTRY_DSN = "${var.sentry_dsn}"
      APP_SERVICE = "${var.project}-dlq"
      APP_TABLE = "${aws_dynamodb_table.dlq_db.id}"
    }
  }
}

resource "aws_cloudwatch_log_group" "dlq_log" {
  name = "/aws/lambda/${aws_lambda_function.dlq_lambda.function_name}"
  retention_in_days = 30
}

output "dlq_lambda" {
  description = "DLQ Lambda"
  value = "${aws_lambda_function.dlq_lambda.function_name}"
}

output "dlq_lambda_arn" {
  description = "DLQ Lambda Arn"
  value = "${aws_lambda_function.dlq_lambda.arn}"
}
