package clouddriveclient

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/koofr/go-httpclient"
)

const (
	InvalidGrantError = "invalid_grant"
)

type RefreshResp struct {
	ExpiresIn   int64  `json:"expires_in"`
	AccessToken string `json:"access_token"`
}

type RefreshRespError struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

type CloudDriveAuth struct {
	ClientId       string
	ClientSecret   string
	RedirectUri    string
	AccessToken    string
	RefreshToken   string
	ExpiresAt      time.Time
	OnTokenRefresh func()
	HTTPClient     httpclient.HTTPClient

	mutex sync.Mutex
}

func (a *CloudDriveAuth) ValidToken() (token string, err error) {
	if time.Now().Unix() > (a.ExpiresAt.Unix() - 5*60) {
		err = a.UpdateRefreshToken()
		if err != nil {
			return "", err
		}
	}

	token = a.AccessToken

	return token, nil
}

func (a *CloudDriveAuth) UpdateRefreshToken() (err error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("client_id", a.ClientId)
	data.Set("client_secret", a.ClientSecret)
	data.Set("redirect_uri", a.RedirectUri)
	data.Set("refresh_token", a.RefreshToken)

	var respVal RefreshResp

	_, err = a.HTTPClient.Request(&httpclient.RequestData{
		Method:         "POST",
		FullURL:        "https://api.amazon.com/auth/o2/token",
		ExpectedStatus: []int{http.StatusOK},
		ReqEncoding:    httpclient.EncodingForm,
		ReqValue:       data,
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &respVal,
	})

	if err != nil {
		err = HandleError(err)

		if cde, ok := IsCloudDriveError(err); ok {
			refreshErr := &RefreshRespError{}
			if jsonErr := json.Unmarshal([]byte(cde.Message), &refreshErr); jsonErr == nil {
				cde.Code = refreshErr.Error
				cde.Message = refreshErr.ErrorDescription
			}
		}

		return err
	}

	a.AccessToken = respVal.AccessToken
	a.ExpiresAt = time.Now().Add(time.Duration(respVal.ExpiresIn) * time.Second)

	if a.OnTokenRefresh != nil {
		a.OnTokenRefresh()
	}

	return nil
}
