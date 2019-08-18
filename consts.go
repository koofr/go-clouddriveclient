package clouddriveclient

const (
	ErrorCodeNoActiveSubscriptionFound = "NO_ACTIVE_SUBSCRIPTION_FOUND"
	ErrorCodeNodeNotFound              = "NODE_NOT_FOUND"
	ErrorCodeParentNodeIDNotFound      = "PARENT_NODE_ID_NOT_FOUND"
	ErrorCodeNameAlreadyExists         = "NAME_ALREADY_EXISTS"
	ErrorCodeCustomerNotFound          = "CUSTOMER_NOT_FOUND"
	ErrorCodeTooManyRequests           = "TOO_MANY_REQUESTS"
)

const (
	NodeKindAsset  = "ASSET"
	NodeKindFile   = "FILE"
	NodeKindFolder = "FOLDER"
	NodeKindGroup  = "GROUP"
)

const (
	NodeStatusAvailable = "AVAILABLE"
	NodeStatusPending   = "PENDING"
	NodeStatusTrash     = "TRASH"
	NodeStatusPurged    = "PURGED"
)
