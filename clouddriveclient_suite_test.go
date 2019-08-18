package clouddriveclient

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGoCloudDriveClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoCloudDriveClient Suite")
}
