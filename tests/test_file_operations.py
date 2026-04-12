import os
import pytest
import tempfile
from file_operations import save_file, list_files, read_file


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def temp_dir():
    """Provide a temporary directory that is cleaned up after each test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


# ── save_file ────────────────────────────────────────────────────────────────

class TestSaveFile:
    def test_creates_file_with_correct_content(self, temp_dir, monkeypatch):
        save_file(temp_dir, "hello.txt", "hello world")
        path = os.path.join(temp_dir, "hello.txt")
        assert os.path.exists(path)
        assert open(path).read() == "hello world"

    def test_creates_directory_if_not_exists(self, temp_dir, monkeypatch):
        new_dir = os.path.join(temp_dir, "subdir", "nested")
        save_file(new_dir, "out.txt", "data")
        assert os.path.isdir(new_dir)

    def test_overwrites_existing_file(self, temp_dir, monkeypatch):
        save_file(temp_dir, "file.txt", "first")
        save_file(temp_dir, "file.txt", "second")
        path = os.path.join(temp_dir, "file.txt")
        assert open(path).read() == "second"

    def test_saves_empty_content(self, temp_dir, monkeypatch):
        save_file(temp_dir, "empty.txt", "")
        path = os.path.join(temp_dir, "empty.txt")
        assert open(path).read() == ""

    def test_saves_unicode_content(self, temp_dir, monkeypatch):
        content = "こんにちは 🌍"
        save_file(temp_dir, "unicode.txt", content)
        path = os.path.join(temp_dir, "unicode.txt")
        assert open(path, encoding="utf-8").read() == content


# ── list_files ───────────────────────────────────────────────────────────────

class TestListFiles:
    def test_returns_empty_list_for_empty_directory(self, temp_dir):
        result = list_files(temp_dir)
        assert result == []

    def test_lists_files_in_flat_directory(self, temp_dir):
        for name in ("a.txt", "b.txt", "c.txt"):
            open(os.path.join(temp_dir, name), "w").close()
        result = list_files(temp_dir)
        relative_paths = [f["relative_path"] for f in result]
        assert relative_paths == ["a.txt", "b.txt", "c.txt"]

    def test_lists_files_recursively(self, temp_dir):
        subdir = os.path.join(temp_dir, "sub")
        os.makedirs(subdir)
        open(os.path.join(temp_dir, "root.txt"), "w").close()
        open(os.path.join(subdir, "child.txt"), "w").close()
        result = list_files(temp_dir)
        relative_paths = [f["relative_path"] for f in result]
        assert "root.txt" in relative_paths
        assert os.path.join("sub", "child.txt") in relative_paths

    def test_results_are_sorted_by_relative_path(self, temp_dir):
        for name in ("z.txt", "a.txt", "m.txt"):
            open(os.path.join(temp_dir, name), "w").close()
        result = list_files(temp_dir)
        paths = [f["relative_path"] for f in result]
        assert paths == sorted(paths)

    def test_each_entry_has_filepath_and_relative_path(self, temp_dir):
        open(os.path.join(temp_dir, "file.txt"), "w").close()
        result = list_files(temp_dir)
        assert len(result) == 1
        assert "filepath" in result[0]
        assert "relative_path" in result[0]

    def test_filepath_is_absolute(self, temp_dir):
        open(os.path.join(temp_dir, "file.txt"), "w").close()
        result = list_files(temp_dir)
        assert os.path.isabs(result[0]["filepath"])


# ── read_file ────────────────────────────────────────────────────────────────

class TestReadFile:
    def test_reads_file_content(self, temp_dir):
        path = os.path.join(temp_dir, "sample.txt")
        open(path, "w", encoding="utf-8").write("some content")
        result = read_file(path)
        assert result["content"] == "some content"

    def test_returns_correct_keys(self, temp_dir):
        path = os.path.join(temp_dir, "sample.txt")
        open(path, "w").close()
        result = read_file(path)
        assert set(result.keys()) == {"filepath", "content", "size_bytes"}

    def test_filepath_is_absolute(self, temp_dir):
        path = os.path.join(temp_dir, "sample.txt")
        open(path, "w").close()
        result = read_file(path)
        assert os.path.isabs(result["filepath"])

    def test_size_bytes_is_correct(self, temp_dir):
        path = os.path.join(temp_dir, "sample.txt")
        content = "hello"
        open(path, "w", encoding="utf-8").write(content)
        result = read_file(path)
        assert result["size_bytes"] == os.path.getsize(path)

    def test_reads_unicode_content(self, temp_dir):
        path = os.path.join(temp_dir, "unicode.txt")
        content = "日本語テスト 🎉"
        open(path, "w", encoding="utf-8").write(content)
        result = read_file(path)
        assert result["content"] == content

    def test_raises_file_not_found(self, temp_dir):
        missing = os.path.join(temp_dir, "missing.txt")
        with pytest.raises(FileNotFoundError):
            read_file(missing)

    def test_raises_value_error_for_directory(self, temp_dir):
        with pytest.raises(ValueError):
            read_file(temp_dir)
