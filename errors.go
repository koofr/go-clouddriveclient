package clouddriveclient

import (
	"encoding/json"
	"github.com/koofr/go-httpclient"
)

type CloudDriveError struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	Logref          string `json:"logref"`
	HttpClientError *httpclient.InvalidStatusError
}

func (e *CloudDriveError) Error() string {
	return e.Message
}

func IsCloudDriveError(err error) (cloudDriveErr *CloudDriveError, ok bool) {
	if cde, ok := err.(*CloudDriveError); ok {
		return cde, true
	} else {
		return nil, false
	}
}

func HandleError(err error) error {
	if ise, ok := httpclient.IsInvalidStatusError(err); ok {
		cloudDriveErr := &CloudDriveError{}

		if ise.Headers.Get("Content-Type") == "application/vnd.error+json" {
			if jsonErr := json.Unmarshal([]byte(ise.Content), &cloudDriveErr); jsonErr != nil {
				cloudDriveErr.Code = "unknown"
				cloudDriveErr.Message = ise.Content
			}
		} else {
			cloudDriveErr.Code = "unknown"
			cloudDriveErr.Message = ise.Content
		}

		cloudDriveErr.HttpClientError = ise

		return cloudDriveErr
	} else {
		return err
	}
}
