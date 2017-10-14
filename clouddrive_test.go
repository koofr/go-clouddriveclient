package clouddriveclient

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/koofr/go-ioutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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

		client, err = NewCloudDrive(auth, http.DefaultClient)
		Expect(err).NotTo(HaveOccurred())

		root, err = client.LookupRoot()
		Expect(err).NotTo(HaveOccurred())
	})

	var createFolder = func() *Node {
		name := fmt.Sprintf("%d", rand.Int())

		node, err := client.CreateFolder(root.Id, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(node.Name).To(Equal(name))

		return node
	}

	Describe("LookupRoot", func() {
		It("should get root node", func() {
			node, err := client.LookupRoot()
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(""))
		})
	})

	Describe("LookupNode", func() {
		It("should find node by parent id and name", func() {
			folder := createFolder()

			for i := 0; i < 5; i++ {
				time.Sleep(2 * time.Second)

				node, ok, lookupError := client.LookupNode(root.Id, folder.Name)
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

			nodes, err := client.NodeChildren(root.Id)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(nodes) > 0).To(BeTrue())
		})
	})

	Describe("Changes", func() {
		It("should get all changes", func() {
			createFolder()

			changes, err := client.Changes("")
			Expect(err).NotTo(HaveOccurred())
			Expect(len(changes.Nodes) > 0).To(BeTrue())
			Expect(changes.Reset).To(BeTrue())

			changes, err = client.Changes(changes.Checkpoint)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(changes.Nodes) == 0).To(BeTrue())
			Expect(changes.Reset).To(BeFalse())

			createFolder()

			changes, err = client.Changes(changes.Checkpoint)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(changes.Nodes) > 0).To(BeTrue())
			Expect(changes.Reset).To(BeFalse())
		})
	})

	Describe("CreateFolder", func() {
		It("should create folder with parent id and name", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.CreateFolder(root.Id, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
		})
	})

	Describe("DeleteNode", func() {
		It("should delete node", func() {
			folder := createFolder()

			_, err := client.DeleteNode(folder.Id)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("RenameNode", func() {
		It("should rename node", func() {
			folder := createFolder()
			newName := fmt.Sprintf("%d", rand.Int())

			node, err := client.RenameNode(folder.Id, newName)
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(newName))
		})
	})

	Describe("MoveNode", func() {
		It("should move node", func() {
			folder := createFolder()
			dest := createFolder()

			node, err := client.MoveNode(folder.Id, root.Id, dest.Id)
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(folder.Name))
		})
	})

	Describe("DownloadNode", func() {
		It("should download node", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, size, err := client.DownloadNode(node.Id, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(reader).NotTo(BeNil())
			Expect(size).To(Equal(int64(5)))

			data, _ := ioutil.ReadAll(reader)
			reader.Close()

			Expect(string(data)).To(Equal("12345"))
		})

		It("should download node range", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, size, err := client.DownloadNode(node.Id, &ioutils.FileSpan{2, 3})
			Expect(err).NotTo(HaveOccurred())
			Expect(reader).NotTo(BeNil())
			Expect(size).To(Equal(int64(2)))

			data, _ := ioutil.ReadAll(reader)
			reader.Close()

			Expect(string(data)).To(Equal("34"))
		})
	})

	Describe("UploadNode", func() {
		It("should upload node", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
			Expect(node.ContentProperties.Size).To(Equal(int64(5)))
		})
	})

	Describe("OverwriteNode", func() {
		It("should overwrite node", func() {
			name := fmt.Sprintf("%d", rand.Int())

			node, err := client.UploadNode(root.Id, name, strings.NewReader("12345"))
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
			Expect(node.ContentProperties.Size).To(Equal(int64(5)))

			time.Sleep(2 * time.Second)

			node, err = client.OverwriteNode(node.Id, strings.NewReader("abc"))
			Expect(err).NotTo(HaveOccurred())
			Expect(node.Name).To(Equal(name))
			Expect(node.ContentProperties.Size).To(Equal(int64(3)))
		})
	})

	Describe("Quota", func() {
		It("should get account quota", func() {
			quota, err := client.Quota()
			Expect(err).NotTo(HaveOccurred())

			Expect(quota.Quota).To(BeNumerically(">", 0))
			Expect(quota.Available).To(BeNumerically(">=", 0))
		})
	})

})
