package webhook

// ExposedServer wraps Server to expose internal methods for testing.
type ExposedServer struct {
	Server *Server
}
