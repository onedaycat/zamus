package ses

import (
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ses"
)

func PublishTemplate(sesClient *ses.SES, tmplName string, filePath string) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	subj, tmpl := getSplitSubjectAndTemplte(string(data))

	_, err = sesClient.CreateTemplate(&ses.CreateTemplateInput{
		Template: &ses.Template{
			HtmlPart:     &tmpl,
			SubjectPart:  &subj,
			TemplateName: &tmplName,
		},
	})

	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if ok && awsErr.Code() == ses.ErrCodeAlreadyExistsException {
			if _, err = sesClient.UpdateTemplate(&ses.UpdateTemplateInput{
				Template: &ses.Template{
					HtmlPart:     &tmpl,
					SubjectPart:  &subj,
					TemplateName: &tmplName,
				},
			}); err != nil {
				panic(err)
			}
			return
		}

		panic(err)
	}
}

func getSplitSubjectAndTemplte(data string) (string, string) {
	idx := strings.Index(data, "\n")

	subj := data[:idx]
	subj = strings.TrimSpace(subj)
	subj = strings.TrimPrefix(subj, "<!--")
	subj = strings.TrimSuffix(subj, "-->")
	subj = strings.TrimSpace(subj)
	tmpl := strings.TrimSpace(data[idx+1:])

	return subj, tmpl
}
