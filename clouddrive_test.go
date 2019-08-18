package clouddriveclient

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/koofr/go-ioutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type testRoundTripper struct {
}

func (t *testRoundTripper) RoundTrip(req *http.Request) (res *http.Response, err error) {
	res, err = http.DefaultTransport.RoundTrip(req)
	time.Sleep(300 * time.Millisecond)
	return res, err
}

var _ = Describe("CloudDrive", func() {
	var client *CloudDrive
	var root *Node

	auth := &CloudDriveAuth{
		ClientId:     os.Getenv("CLOUDDRIVE_CLIENT_ID"),
		ClientSecret: os.Getenv("CLOUDDRIVE_CLIENT_SECRET"),
		RedirectUri:  os.Getenv("CLOUDDRIVE_REDIRECT_URI"),
		AccessToken:  os.Getenv("CLOUDDRIVE_ACCESS_TOKEN"),
		RefreshToken: os.Getenv("CLOUDDRIVE_REFRESH_TOKEN"),
	}

	if auth.ClientId == "" || auth.ClientSecret == "" || auth.RedirectUri == "" || auth.AccessToken == "" || auth.RefreshToken == "" || os.Getenv("CLOUDDRIVE_EXPIRES_AT") == "" {
		fmt.Println("CLOUDDRIVE_CLIENT_ID, CLOUDDRIVE_CLIENT_SECRET, CLOUDDRIVE_ACCESS_TOKEN, CLOUDDRIVE_REFRESH_TOKEN, CLOUDDRIVE_EXPIRES_AT env variable missing")
		return
	}

	exp, _ := strconv.ParseInt(os.Getenv("CLOUDDRIVE_EXPIRES_AT"), 10, 0)
	auth.ExpiresAt = time.Unix(0, exp*1000000)

	BeforeEach(func() {
		var err error

		rand.Seed(time.Now().UnixNano())

		httpClient := &http.Client{
			// we need a custom transport that adds some delay otherwise we get random read after
			// write errors (e.g. Info after Delete succeeds)
			Transport: &testRoundTripper{},
		}

		client, err = NewCloudDrive(auth, httpClient)
		Expect(err).NotTo(HaveOccurred())

		endpoint, err := client.GetEndpoint(context.Background())
		Expect(err).NotTo(HaveOccurred())
		client.InitEndpoint(endpoint.ContentUrl, endpoint.MetadataUrl)

		root, err = client.LookupRoot(context.Background())
		Expect(err).NotTo(HaveOccurred())
	})

	var createFolder = func() *Node {
		name := fmt.Sprintf("%d", rand.Int())

		node, err := client.CreateFolder(context.Background(), root.Id, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(node.Name).To(Equal(name))

		return node
	}

	Describe("LookupRoot", func() {
		It("should get root node", func() {
			node, err := client.LookupRoot(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(""))
		})
	})

	Describe("LookupNode", func() {
		It("should find node by parent id and name", func() {
			folder := createFolder()

			for i := 0; i < 5; i++ {
				time.Sleep(2 * time.Second)

				node, ok, lookupError := client.LookupNode(context.Background(), root.Id, folder.Name)
				if ok {
					Expect(node.Name).To(Equal(folder.Name))
					Expect(node.Id).To(Equal(folder.Id))

					break
				}

				if i == 4 && lookupError != nil {
					Expect(lookupError).NotTo(HaveOccurred())
				}
			}
		})
	})

	Describe("NodeChildren", func() {
		It("should get nodes for parent id", func() {
			createFolder()

			nodes, err := client.NodeChildren(context.Background(), root.Id)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(nodes) > 0).To(BeTrue())
		})

		It("should fail with node not found error", func() {
			_, err := client.NodeChildren(context.Background(), "nonexistentid")
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})
	})

	Describe("Changes", func() {
		It("should get all changes", func() {
			createFolder()

			changes, err := client.Changes(context.Background(), "")
			Expect(err).NotTo(HaveOccurred())
			Expect(len(changes.Nodes) > 0).To(BeTrue())
			Expect(changes.Reset).To(BeTrue())

			changes, err = client.Changes(context.Background(), changes.Checkpoint)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(changes.Nodes) == 0).To(BeTrue())
			Expect(changes.Reset).To(BeFalse())

			createFolder()

			changes, err = client.Changes(context.Background(), changes.Checkpoint)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(changes.Nodes) > 0).To(BeTrue())
			Expect(changes.Reset).To(BeFalse())
		})
	})

	Describe("CreateFolder", func() {
		It("should create a folder with parent id and name", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.CreateFolder(context.Background(), root.Id, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
		})

		It("should not create a folder with parent id and existing name", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.CreateFolder(context.Background(), root.Id, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))

			_, err = client.CreateFolder(context.Background(), root.Id, name)
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNameAlreadyExists))
			Expect(cde.Message).To(MatchRegexp(`^Node with the name \w+ already exists under parentId \w+ conflicting NodeId: \w+`))
		})
	})

	Describe("DeleteNode", func() {
		It("should delete a node", func() {
			folder := createFolder()

			_, err := client.DeleteNode(context.Background(), folder.Id)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should not delete a non-existent node", func() {
			_, err := client.DeleteNode(context.Background(), "nonexistentid")
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})
	})

	Describe("RenameNode", func() {
		It("should rename a node", func() {
			folder := createFolder()
			newName := fmt.Sprintf("%d", rand.Int())

			node, err := client.RenameNode(context.Background(), folder.Id, newName)
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(newName))
		})

		It("should not rename a non-existent node", func() {
			newName := fmt.Sprintf("%d", rand.Int())

			_, err := client.RenameNode(context.Background(), "nonexistentid", newName)
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})

		It("should not rename a node if the name already exists", func() {
			folder := createFolder()
			existingFolder := createFolder()

			_, err := client.RenameNode(context.Background(), folder.Id, existingFolder.Name)
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNameAlreadyExists))
			Expect(cde.Message).To(MatchRegexp(`^Node with the name \w+ already exists under parentId \w+ conflicting NodeId: \w+`))
		})
	})

	Describe("MoveNode", func() {
		It("should move a node", func() {
			folder := createFolder()
			dest := createFolder()

			node, err := client.MoveNode(context.Background(), folder.Id, root.Id, dest.Id)
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(folder.Name))
		})

		It("should not move a non-existent node", func() {
			dest := createFolder()

			_, err := client.MoveNode(context.Background(), "nonexistentid", root.Id, dest.Id)
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})

		It("should not move a node to a non-existent destination", func() {
			folder := createFolder()

			_, err := client.MoveNode(context.Background(), folder.Id, root.Id, "nonexistentid")
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})

		It("should not move a node from a non-existent parent", func() {
			folder := createFolder()
			dest := createFolder()

			_, err := client.MoveNode(context.Background(), folder.Id, "nonexistentid", dest.Id)
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})
	})

	Describe("DownloadNode", func() {
		It("should download a node", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(context.Background(), root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, size, err := client.DownloadNode(context.Background(), node.Id, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(reader).NotTo(BeNil())
			Expect(size).To(Equal(int64(5)))

			data, _ := ioutil.ReadAll(reader)
			reader.Close()

			Expect(string(data)).To(Equal("12345"))
		})

		It("should download a node range", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(context.Background(), root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, size, err := client.DownloadNode(context.Background(), node.Id, &ioutils.FileSpan{2, 3})
			Expect(err).NotTo(HaveOccurred())
			Expect(reader).NotTo(BeNil())
			Expect(size).To(Equal(int64(2)))

			data, _ := ioutil.ReadAll(reader)
			reader.Close()

			Expect(string(data)).To(Equal("34"))
		})

		It("should download a node by temp link", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(context.Background(), root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, size, err := client.DownloadNodeByTempLink(context.Background(), node.Id, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(reader).NotTo(BeNil())
			Expect(size).To(Equal(int64(5)))

			data, _ := ioutil.ReadAll(reader)
			reader.Close()

			Expect(string(data)).To(Equal("12345"))
		})

		It("should download a node range by temp link", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(context.Background(), root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, size, err := client.DownloadNodeByTempLink(context.Background(), node.Id, &ioutils.FileSpan{2, 3})
			Expect(err).NotTo(HaveOccurred())
			Expect(reader).NotTo(BeNil())
			Expect(size).To(Equal(int64(2)))

			data, _ := ioutil.ReadAll(reader)
			reader.Close()

			Expect(string(data)).To(Equal("34"))
		})

		It("should not download a non-existent node", func() {
			_, _, err := client.DownloadNode(context.Background(), "nonexistentid", nil)
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})
	})

	Describe("UploadNode", func() {
		It("should upload a node", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(context.Background(), root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
			Expect(node.ContentProperties.Size).To(Equal(int64(5)))
		})

		It("should not upload a node to a non-existent parent", func() {
			name := fmt.Sprintf("%d", rand.Int())

			_, err := client.UploadNode(context.Background(), "nonexistentid", name, strings.NewReader("12345"))
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeParentNodeIDNotFound))
			Expect(cde.Message).To(Equal("One of the parentId doesn't exists"))
		})
	})

	Describe("OverwriteNode", func() {
		It("should overwrite a node", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(context.Background(), root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
			Expect(node.ContentProperties.Size).To(Equal(int64(5)))

			time.Sleep(2 * time.Second)

			node, err = client.OverwriteNode(context.Background(), node.Id, strings.NewReader("abc"))
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
			Expect(node.ContentProperties.Size).To(Equal(int64(3)))
		})

		It("should not overwrite a non-existent node", func() {
			_, err := client.OverwriteNode(context.Background(), "nonexistentid", strings.NewReader("abc"))
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeNodeNotFound))
			Expect(cde.Message).To(Equal("Node does not exists"))
		})
	})

	Describe("Quota", func() {
		It("should get account quota", func() {
			quota, err := client.Quota(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Expect(quota.Quota).To(BeNumerically(">", 0))
			Expect(quota.Available).To(BeNumerically(">=", 0))
		})
	})

	Describe("Errors", func() {
		It("should handle Too many requests error", func() {
			client.MaxRetries = 2
			folder := createFolder()

			retries := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				retries++

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"logref":"LOGREF-UUID","message":"Rate exceeded","code":""}`))
			}))
			defer server.Close()
			baseURL, _ := url.Parse(server.URL)
			client.MetadataClient.BaseURL = baseURL

			_, err := client.NodeChildren(context.Background(), folder.Id)
			Expect(err).To(HaveOccurred())
			cde, ok := IsCloudDriveError(err)
			Expect(ok).To(BeTrue())
			Expect(cde.Code).To(Equal(ErrorCodeTooManyRequests))
			Expect(cde.Message).To(Equal("Rate exceeded"))
			Expect(retries).To(Equal(2))
		})

		It("should retry on Too many requests error", func() {
			client.MaxRetries = 3
			folder := createFolder()

			retries := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				retries++

				if retries == 3 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"data":[],"count":0}`))
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusTooManyRequests)
					w.Write([]byte(`{"logref":"LOGREF-UUID","message":"Rate exceeded","code":""}`))
				}
			}))
			defer server.Close()
			baseURL, _ := url.Parse(server.URL)
			client.MetadataClient.BaseURL = baseURL

			children, err := client.NodeChildren(context.Background(), folder.Id)
			Expect(err).NotTo(HaveOccurred())
			Expect(children).To(BeEmpty())
			Expect(retries).To(Equal(3))
		})
	})
})
