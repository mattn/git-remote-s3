package main

import gits3 "github.com/mattn/git-remote-s3"

const name = "git-remote-s3"
const version = "0.0.0"

var revision = "HEAD"

func main() {
	gits3.Name = name
	gits3.Version = version
	gits3.Revision = revision
	gits3.Main()
}
