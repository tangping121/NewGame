package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMigrationFilesUseNumericOrder(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"migrate_v10.sql", "init.sql", "migrate_v2.sql"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	files, err := migrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"init.sql", "migrate_v2.sql", "migrate_v10.sql"}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("migration order = %v, want %v", files, want)
	}
}

func TestMigrationFilesRejectAmbiguousName(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "migrate_latest.sql"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := migrationFiles(dir); err == nil {
		t.Fatal("expected invalid migration name to fail")
	}
}

func TestUniqueDSNsAndRoleShard(t *testing.T) {
	got := uniqueDSNs(" central ", "a, central, b, a")
	want := []string{"central", "a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unique DSNs = %v, want %v", got, want)
	}
	if got := roleShard(-1, 50); got != 49 {
		t.Fatalf("roleShard(-1, 50) = %d, want 49", got)
	}
}
