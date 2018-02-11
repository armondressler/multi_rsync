import pytest
from os import path,mkdir
from shutil import rmtree
from multi_rsync.multi_rsync import RemoteConnect

top_dir_original = "/tmp/testrun_original"
top_dir_copy = "/tmp/testrun_copy"
rcon = RemoteConnect("localhost",
                     "taadrar1",
                     "/tmp/testrun_original",
                     "/tmp/testrun_copy",
                     testmode=True)
expected_tree = ["top", "mid1", "bot111", "botbot1", "file1"]
dir_structure = {"top": {
    "mid1": {
        "bot1": ["file1", "file2"],
        "bot11": ["file1", "file2"],
        "bot111": {
            "botbot1": ["file1", "file2"]
        }
    },
    "mid2": {
        "bot2": [],
        "bot22": ["file1", "file2", "file3"]
    },
    "mid3": {
        "bot3": ["file1", "file2", "file3", "file4"]
    },
    "mid4": ["file1","file2"],
    "mid5": ["file1","file2"]
    }}

def create_dirstructure(top_dir,structure):
    try:
        for dir in structure.keys():
            mkdir(path.join(top_dir, dir))
            create_dirstructure(path.join(top_dir,dir),structure[dir])
    except AttributeError:
        for file in structure:
            open(path.join(top_dir,file),"wb")

@pytest.fixture
def prepare_dirs():
    try:
        rmtree(top_dir_original)
        rmtree(top_dir_copy)
    except FileNotFoundError:
        pass
    try:
        mkdir(top_dir_original)
    except FileExistsError:
        pass
    create_dirstructure(top_dir_original,dir_structure)


def test(prepare_dirs):
    for tree_length in range(len(expected_tree)):
        rcon.max_depth = tree_length
        rcon._get_dir()
        expected_dirs = path.join(top_dir_copy, *expected_tree[:tree_length])
        if tree_length < len(expected_tree):
            unexpected_dirs = path.join(top_dir_copy,*expected_tree[:tree_length+1])
            assert path.isdir(unexpected_dirs) == False
        assert path.isdir(expected_dirs) == True