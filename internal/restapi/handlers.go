package restapi

import "net/http"

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request)     { notImplemented(w) }
func (s *Server) getProject(w http.ResponseWriter, r *http.Request)       { notImplemented(w) }
func (s *Server) listRepositories(w http.ResponseWriter, r *http.Request) { notImplemented(w) }
func (s *Server) listTasks(w http.ResponseWriter, r *http.Request)        { notImplemented(w) }
func (s *Server) getTask(w http.ResponseWriter, r *http.Request)          { notImplemented(w) }
func (s *Server) patchTask(w http.ResponseWriter, r *http.Request)        { notImplemented(w) }
func (s *Server) listSubtasks(w http.ResponseWriter, r *http.Request)     { notImplemented(w) }
func (s *Server) createSubtask(w http.ResponseWriter, r *http.Request)    { notImplemented(w) }
func (s *Server) patchSubtask(w http.ResponseWriter, r *http.Request)     { notImplemented(w) }

func notImplemented(w http.ResponseWriter) { w.WriteHeader(http.StatusNotImplemented) }
