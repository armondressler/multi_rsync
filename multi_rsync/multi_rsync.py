from anytree import Node, LevelOrderGroupIter
import argparse
from distutils import spawn
from subprocess import call
from os import path, listdir
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())


class FileTree:

    def __init__(self, maxdepth):
        """

        :param maxdepth:
        :type maxdepth:
        """
        self.maxdepth = maxdepth


class RemoteConnect:

    def __init__(self, host, user, remote_path, local_path,
                 port=None, password=None, password_file=None, identityfile=None, testmode=False, maxdepth=2):
        self.host = host
        self.port = port
        self.user = user
        self.remote_path = remote_path
        self.local_path = local_path
        self.password = password
        self.password_file = password_file
        self.identityfile = identityfile
        self.testmode = testmode
        self.max_depth = maxdepth
        self.rsync_binary_path = spawn.find_executable("rsync")
        self.ssh_binary_path = spawn.find_executable("ssh")
        self.top_node = Node(self.local_path)

    def initialize_directories(self):
        self._get_dir(self.max_depth)
        self._map_top_dirs(self.top_node)

    def _get_password_from_file(self):
        with open(self.password_file, "r") as pwfile:
            self.password = pwfile.readline()

    def _map_top_dirs(self, parent):
        parent_path = path.abspath(parent.name)
        for subdir in [element for element in listdir(parent_path) if
                       path.isdir(path.join(parent_path, element))]:
            subdir_node = Node(path.join(parent_path, subdir), parent=parent)
            self._map_top_dirs(subdir_node)

    def _translate_depth_to_string(self, depth):
        rsync_arg = ["/*"]
        for _ in range(depth):
            rsync_arg.append("/*")
        return "".join(rsync_arg)

    def _get_dir(self, depth=None):
        ssh_command = ""
        if self.identityfile:
            ssh_command = "-e \"{} -i {}\"".format(self.ssh_binary_path, self.identityfile)
        exclude_command = ""
        if depth is not None:
            logger.info("depth is {}".format(depth))
            depth = self._translate_depth_to_string(depth)
            exclude_command = "--exclude=\"{}\"".format(depth)

        if not self.testmode:
            rsync_call = "{rsync} -ar {ssh_com} {exclude_com} {user}@{host}:{remote_path}/ {local_path}".format(
                rsync=self.rsync_binary_path,
                exclude_depth=depth,
                user=self.user,
                host=self.host,
                ssh_com=ssh_command,
                exclude_com=exclude_command,
                remote_path=self.remote_path,
                local_path=self.local_path)
        else:
            rsync_call = "{rsync} -ar {exclude_com} {remote_path}/ {local_path}".format(
                rsync=self.rsync_binary_path,
                exclude_com=exclude_command,
                remote_path=self.remote_path,
                local_path=self.local_path)

        logger.info("calling rsync with {}".format(rsync_call))
        call(rsync_call, shell=True)


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--host", "-h", action="store", required=True)
    parser.add_argument("--port", "-p", action="store", type=int, default=22)
    parser.add_argument("--identity-file", "-i", action="store")
    parser.add_argument("--user", "-u", action="store", required=True)
    parser.add_argument("--password", action="store")
    parser.add_argument("--password-file", action="store")
    parser.add_argument("--max-processes", action="store", type=int)
    parser.add_argument("--remote-dir", "-r", action="store")
    parser.add_argument("--local-dir", "-l", action="store")
    parser.add_argument("--max-depth", '-d', action="store", type=int)
    parser.add_argument("--max-threads", "-t", action="store", type=int)
    parser.add_argument("--testmode", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    rcon = RemoteConnect(args.host,
                         args.user,
                         args.remote_dir,
                         args.local_dir,
                         port=args.port,
                         password=args.password,
                         password_file=args.password_file,
                         identityfile=args.identity_file,
                         testmode=args.testmode)
    rcon.initialize_directories()
    print([[node.name for node in children] for children in LevelOrderGroupIter(rcon.top_node)])


if __name__ == '__main__':
    main()
