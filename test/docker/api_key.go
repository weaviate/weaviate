package docker

type ApiKeyUser struct {
	Key      string
	Username string
	Admin    bool
	Viewer   bool
}
