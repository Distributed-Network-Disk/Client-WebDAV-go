package lib

import (
	"context"
	"mime"
	"os"
	"path"

	"golang.org/x/net/webdav"
)

// S3 with minio sdk
// type S3conf struct {
// 	Endpoint        string // endpoint := "play.min.io"
// 	AccessKeyID     string // accessKeyID := "Q3AM3UQ867SPQQA43P2F"
// 	SecretAccessKey string // secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
// 	UseSSL          bool   // useSSL := true
// 	Bucket          string
// }

// // TODO: fake file info, replace with all os.FileInfo and WebDavFile
// func S3(conf S3conf) {
// 	minioClient, err := minio.New(conf.Endpoint, &minio.Options{
// 		Creds:  credentials.NewStaticV4(conf.AccessKeyID, conf.SecretAccessKey, ""),
// 		Secure: conf.UseSSL,
// 	})
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	log.Printf("%#v\n", minioClient)
// }

// NoSniffFileInfo wraps any generic FileInfo interface and bypasses mime type sniffing.
type NoSniffFileInfo struct {
	os.FileInfo
}

func (w NoSniffFileInfo) ContentType(ctx context.Context) (contentType string, err error) {
	if mimeType := mime.TypeByExtension(path.Ext(w.FileInfo.Name())); mimeType != "" {
		// We can figure out the mime from the extension.
		return mimeType, nil
	} else {
		// We can't figure out the mime type without sniffing, call it an octet stream.
		return "application/octet-stream", nil
	}
}

type WebDavDir struct {
	webdav.Dir
	NoSniff bool
}

func (d WebDavDir) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	// Skip wrapping if NoSniff is off
	if !d.NoSniff {
		return d.Dir.Stat(ctx, name)
	}

	info, err := d.Dir.Stat(ctx, name)
	if err != nil {
		return nil, err
	}

	return NoSniffFileInfo{info}, nil
}

func (d WebDavDir) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	// Skip wrapping if NoSniff is off
	if !d.NoSniff {
		return d.Dir.OpenFile(ctx, name, flag, perm)
	}

	file, err := d.Dir.OpenFile(ctx, name, flag, perm)
	if err != nil {
		return nil, err
	}

	return WebDavFile{File: file}, nil
}

type WebDavFile struct {
	webdav.File
}

func (f WebDavFile) Stat() (os.FileInfo, error) {
	info, err := f.File.Stat()
	if err != nil {
		return nil, err
	}

	return NoSniffFileInfo{info}, nil
}

func (f WebDavFile) Readdir(count int) (fis []os.FileInfo, err error) {
	fis, err = f.File.Readdir(count)
	if err != nil {
		return nil, err
	}

	for i := range fis {
		fis[i] = NoSniffFileInfo{fis[i]}
	}
	return fis, nil
}
