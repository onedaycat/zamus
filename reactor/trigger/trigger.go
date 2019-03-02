package trigger

import (
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"github.com/onedaycat/zamus/common"
)

func Init() {
	common.PrettyLog()
	xray.SetLogger(xraylog.NullLogger)
}
