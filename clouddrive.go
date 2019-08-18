package clouddriveclient

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/koofr/go-httpclient"
	"github.com/koofr/go-ioutils"
)

const DefaultMaxRetries = 5

type CloudDrive struct {
	HTTPClient     *http.Client
	EndpointClient *httpclient.HTTPClient
	Auth           *CloudDriveAuth
	MaxRetries     int

	ContentClient  *httpclient.HTTPClient
	MetadataClient *httpclient.HTTPClient
}

func NewCloudDrive(auth *CloudDriveAuth, httpClient *http.Client) (d *CloudDrive, err error) {
	authHTTPClient := httpclient.New()
	authHTTPClient.Client = httpClient
	auth.HTTPClient = authHTTPClient

	endpointURL, _ := url.Parse("https://drive.amazonaws.com/drive/v1")

	endpointClient := httpclient.New()
	endpointClient.Client = httpClient
	endpointClient.BaseURL = endpointURL

	d = &CloudDrive{
		HTTPClient:     httpClient,
		EndpointClient: endpointClient,
		Auth:           auth,
		MaxRetries:     DefaultMaxRetries,
	}

	return d, nil
}

func (d *CloudDrive) HandleError(err error) error {
	return HandleError(err)
}

func (d *CloudDrive) InitEndpoint(contentURL string, metadataURL string) error {
	contentUrl, err := url.Parse(contentURL)
	if err != nil {
		return err
	}
	metadataUrl, err := url.Parse(metadataURL)
	if err != nil {
		return err
	}

	d.ContentClient = httpclient.New()
	d.ContentClient.Client = d.HTTPClient
	d.ContentClient.BaseURL = contentUrl

	d.MetadataClient = httpclient.New()
	d.MetadataClient.Client = d.HTTPClient
	d.MetadataClient.BaseURL = metadataUrl

	return nil
}

func (d *CloudDrive) Request(client *httpclient.HTTPClient, request *httpclient.RequestData) (response *http.Response, err error) {
	retries := d.MaxRetries

	canRetry := request.CanCopy()

	if !canRetry {
		retries = 1
	}

	authCtx := request.Context
	if authCtx == nil {
		authCtx = context.Background()
	}

	for retry := 0; retry < retries; retry++ {
		var currentRequest *httpclient.RequestData

		if canRetry {
			_, currentRequest = request.Copy()
		} else {
			currentRequest = request
		}

		token, err := d.Auth.ValidToken(authCtx)
		if err != nil {
			return nil, err
		}

		if currentRequest.Headers == nil {
			currentRequest.Headers = http.Header{}
		}

		currentRequest.Headers.Set("Authorization", "Bearer "+token)

		response, err = client.Request(currentRequest)

		if err != nil {
			if httpErr, ok := err.(httpclient.InvalidStatusError); ok {
				if httpErr.Got == http.StatusTooManyRequests && retry+1 < retries {
					seconds := rand.Intn(int(math.Pow(2, float64(retry))))

					time.Sleep(time.Duration(seconds) * time.Second)

					continue
				}
			}

			return nil, d.HandleError(err)
		}

		return response, nil
	}

	panic("unreachable")
}

func (d *CloudDrive) MetadataRequest(request *httpclient.RequestData) (response *http.Response, err error) {
	if d.MetadataClient == nil {
		return nil, fmt.Errorf("metadata client not initialized")
	}
	return d.Request(d.MetadataClient, request)
}

func (d *CloudDrive) ContentRequest(request *httpclient.RequestData) (response *http.Response, err error) {
	if d.ContentClient == nil {
		return nil, fmt.Errorf("content client not initialized")
	}
	return d.Request(d.ContentClient, request)
}

func (d *CloudDrive) GetEndpoint(ctx context.Context) (e *Endpoint, err error) {
	e = &Endpoint{}

	_, err = d.Request(d.EndpointClient, &httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           "/account/endpoint",
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &e,
	})

	if err != nil {
		return nil, err
	}

	if !e.CustomerExists {
		return nil, ErrCustomerNotFound
	}

	return e, nil
}

func (d *CloudDrive) LookupRoot(ctx context.Context) (root *Node, err error) {
	params := make(url.Values)
	params.Set("filters", "isRoot:true")

	nodes := &Nodes{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           "/nodes",
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &nodes,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, err
	}

	if len(nodes.Nodes) == 0 {
		return nil, ErrRootNotFound
	}

	root = nodes.Nodes[0]

	return root, nil
}

func (d *CloudDrive) LookupNode(ctx context.Context, parentId string, name string) (node *Node, ok bool, err error) {
	nameEscaped := strings.Replace(name, "\"", "\\\\", -1)

	params := make(url.Values)
	params.Set("filters", "parents:"+parentId+" AND name:\""+nameEscaped+"\"")

	nodes := &Nodes{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           "/nodes",
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &nodes,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, false, err
	}

	if len(nodes.Nodes) == 0 {
		return nil, false, nil
	}

	return nodes.Nodes[0], true, nil
}

func (d *CloudDrive) LookupNodeById(ctx context.Context, nodeId string) (node *Node, err error) {
	params := make(url.Values)
	params.Set("tempLink", "true")

	node = &Node{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           "/nodes/" + nodeId,
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, err
	}
	return node, nil
}

func (d *CloudDrive) NodeChildren(ctx context.Context, parentId string) (nodes []*Node, err error) {
	nextToken := ""

	nodes = []*Node{}

	for {
		params := make(url.Values)
		if nextToken != "" {
			params.Set("startToken", nextToken)
		}

		ns := &Nodes{}

		req := &httpclient.RequestData{
			Context:        ctx,
			Method:         "GET",
			Path:           "/nodes/" + parentId + "/children",
			Params:         params,
			ExpectedStatus: []int{http.StatusOK},
			RespEncoding:   httpclient.EncodingJSON,
			RespValue:      &ns,
		}

		_, err = d.MetadataRequest(req)

		if err != nil {
			return nil, err
		}

		if len(ns.Nodes) == 0 {
			break
		}

		nodes = append(nodes, ns.Nodes...)

		if ns.NextToken == "" {
			break
		}

		nextToken = ns.NextToken
	}

	return nodes, nil
}

func (d *CloudDrive) Changes(ctx context.Context, checkpoint string) (changes *Changes, err error) {
	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "POST",
		Path:           "/changes",
		ExpectedStatus: []int{http.StatusOK},
	}

	if checkpoint != "" {
		req.ReqEncoding = httpclient.EncodingJSON

		req.ReqValue = struct {
			Checkpoint string `json:"checkpoint"`
		}{
			Checkpoint: checkpoint,
		}
	}

	res, err := d.MetadataRequest(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	r := res.Body

	if res.Header.Get("Content-Encoding") == "gzip" {
		r, err = gzip.NewReader(r)

		if err != nil {
			return nil, err
		}
	}

	decoder := json.NewDecoder(r)

	changes = &Changes{}

	err = decoder.Decode(changes)

	if err != nil {
		return nil, err
	}

	return changes, nil
}

func (d *CloudDrive) CreateFolder(ctx context.Context, parentId string, name string) (node *Node, err error) {
	create := &NodeCreate{
		Name:    name,
		Kind:    NodeKindFolder,
		Parents: []string{parentId},
	}

	node = &Node{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "POST",
		Path:           "/nodes",
		ExpectedStatus: []int{http.StatusCreated},
		ReqEncoding:    httpclient.EncodingJSON,
		ReqValue:       create,
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) DeleteNode(ctx context.Context, nodeId string) (node *Node, err error) {
	node = &Node{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           "/trash/" + nodeId,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) RenameNode(ctx context.Context, nodeId string, newName string) (node *Node, err error) {
	rename := &NodeRename{
		Name: newName,
	}

	node = &Node{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "PATCH",
		Path:           "/nodes/" + nodeId,
		ExpectedStatus: []int{http.StatusOK},
		ReqEncoding:    httpclient.EncodingJSON,
		ReqValue:       rename,
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) MoveNode(ctx context.Context, nodeId string, fromParentId string, toParentId string) (node *Node, err error) {
	move := &NodeMove{
		FromParent: fromParentId,
		ChildId:    nodeId,
	}

	node = &Node{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "POST",
		Path:           "/nodes/" + toParentId + "/children",
		ExpectedStatus: []int{http.StatusOK},
		ReqEncoding:    httpclient.EncodingJSON,
		ReqValue:       move,
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) DownloadNode(ctx context.Context, nodeId string, span *ioutils.FileSpan) (reader io.ReadCloser, size int64, err error) {
	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           "/nodes/" + nodeId + "/content",
		ExpectedStatus: []int{http.StatusOK, http.StatusPartialContent},
	}

	if span != nil {
		req.Headers = make(http.Header)
		req.Headers.Set("Range", fmt.Sprintf("bytes=%d-%d", span.Start, span.End))
	}

	res, err := d.ContentRequest(req)

	if err != nil {
		return nil, 0, err
	}

	return res.Body, res.ContentLength, nil
}

func (d *CloudDrive) DownloadNodeByTempLink(ctx context.Context, nodeId string, span *ioutils.FileSpan) (reader io.ReadCloser, size int64, err error) {
	node, err := d.LookupNodeById(ctx, nodeId)
	if err != nil {
		return nil, 0, err
	}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		FullURL:        node.TempLink,
		ExpectedStatus: []int{http.StatusOK, http.StatusPartialContent},
	}
	if span != nil {
		req.Headers = make(http.Header)
		req.Headers.Set("Range", fmt.Sprintf("bytes=%d-%d", span.Start, span.End))
	}

	res, err := d.ContentRequest(req)

	if err != nil {
		return nil, 0, err
	}

	return res.Body, res.ContentLength, nil
}

func (d *CloudDrive) UploadNode(ctx context.Context, parentId string, name string, reader io.Reader) (node *Node, err error) {
	create := &NodeCreate{
		Name:    name,
		Kind:    NodeKindFile,
		Parents: []string{parentId},
	}

	createJson, err := json.Marshal(create)

	if err != nil {
		return nil, err
	}

	params := make(url.Values)
	params.Set("suppress", "deduplication")

	node = &Node{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "POST",
		Path:           "/nodes",
		Params:         params,
		ExpectedStatus: []int{http.StatusCreated},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	extra := map[string]string{
		"metadata": string(createJson),
	}

	err = req.UploadFileExtra("file", "file", reader, extra)

	if err != nil {
		return nil, err
	}

	_, err = d.ContentRequest(req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) OverwriteNode(ctx context.Context, nodeId string, reader io.Reader) (node *Node, err error) {
	node = &Node{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           "/nodes/" + nodeId + "/content",
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	err = req.UploadFile("file", "file", reader)

	if err != nil {
		return nil, err
	}

	_, err = d.ContentRequest(req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) Quota(ctx context.Context) (quota *Quota, err error) {
	quota = &Quota{}

	req := &httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           "/account/quota",
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &quota,
	}

	_, err = d.MetadataRequest(req)

	if err != nil {
		return nil, err
	}

	return quota, nil
}
