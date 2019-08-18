package clouddriveclient

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/koofr/go-httpclient"
)

type CloudDriveError struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	Logref          string `json:"logref"`
	HttpClientError *httpclient.InvalidStatusError
}

func (e *CloudDriveError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

var ErrCustomerNotFound = &CloudDriveError{
	Code:            ErrorCodeCustomerNotFound,
	Message:         "Endpoint customer does not exist",
	Logref:          "",
	HttpClientError: nil,
}

var ErrRootNotFound = &CloudDriveError{
	Code:            ErrorCodeNodeNotFound,
	Message:         "Root node not found",
	Logref:          "",
	HttpClientError: nil,
}

func IsCloudDriveError(err error) (cloudDriveErr *CloudDriveError, ok bool) {
	if cde, ok := err.(*CloudDriveError); ok {
		return cde, true
	} else {
		return nil, false
	}
}

func HandleError(err error) error {
	ise, ok := httpclient.IsInvalidStatusError(err)
	if !ok {
		return err
	}

	cloudDriveErr := &CloudDriveError{}

	ct := ise.Headers.Get("Content-Type")
	if ct == "application/vnd.error+json" || ct == "application/json" {
		if jsonErr := json.Unmarshal([]byte(ise.Content), &cloudDriveErr); jsonErr != nil {
			cloudDriveErr.Code = "unknown"
			cloudDriveErr.Message = ise.Content
		}
	} else {
		cloudDriveErr.Code = "unknown"
		cloudDriveErr.Message = ise.Content
	}

	// handle inconsistent Amazon errors (e.g. {"message":"Node does not exists"})
	if ise.Got == http.StatusNotFound && cloudDriveErr.Code == "" && cloudDriveErr.Message == "Node does not exists" {
		cloudDriveErr.Code = ErrorCodeNodeNotFound
	}

	if ise.Got == http.StatusTooManyRequests {
		// JSON body response:
		// {"logref":"LOGREF-UUID","message":"Rate exceeded","code":""}
		cloudDriveErr.Code = ErrorCodeTooManyRequests
		if cloudDriveErr.Message == "" {
			cloudDriveErr.Message = "Rate exceeded"
		}
	}

	cloudDriveErr.HttpClientError = ise

	return cloudDriveErr
}
