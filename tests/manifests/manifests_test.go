package manifests_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"bastion/pkg/config"

	"gopkg.in/yaml.v3"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestServiceConfigsAreStrictAndValid(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(repositoryRoot(t), "configs", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no service configs found")
	}
	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var service config.Service
			decoder := yaml.NewDecoder(bytes.NewReader(data))
			decoder.KnownFields(true)
			if err := decoder.Decode(&service); err != nil {
				t.Fatal(err)
			}
			if err := service.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestKubernetesManifestsAreValidYAML(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(repositoryRoot(t), "deploy", "k8s", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			handle, err := os.Open(file)
			if err != nil {
				t.Fatal(err)
			}
			defer handle.Close()
			decoder := yaml.NewDecoder(handle)
			documents := 0
			for {
				var document struct {
					APIVersion string `yaml:"apiVersion"`
					Kind       string `yaml:"kind"`
				}
				err := decoder.Decode(&document)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if document.APIVersion == "" && document.Kind == "" {
					continue
				}
				if document.APIVersion == "" || document.Kind == "" {
					t.Fatalf("document is missing apiVersion or kind")
				}
				documents++
			}
			if documents == 0 {
				t.Fatal("manifest has no Kubernetes resources")
			}
		})
	}
}
