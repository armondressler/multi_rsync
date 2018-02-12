from anytree import Node, LevelOrderGroupIter
import argparse
from distutils import spawn
from subprocess import call
from os import path, listdir
import logging
from pathos.multiprocessing import ProcessingPool as Pool
from pathos.multiprocessing import cpu_count
import atexit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())


class ProcessPool():
    def __init__(self, maxprocesses):
        self.pool = Pool(processes=maxprocesses)
        logger.info("Initialized process pool of size {}".format(maxprocesses))

    def start(self, fuction, local_dirs, remote_dirs):
        atexit.register(self.exit)
        logger.info("Started with parallelization")
        results = self.pool.map(fuction, list(zip(local_dirs, remote_dirs)))
        logger.info("Stopped with results: {}".format(results))
        return results

    def exit(self):
        self.pool.close()
        self.pool.join()


class RemoteConnect:

    def __init__(self, host, user, remote_path, local_path,
                 port=None, password=None, password_file=None, identityfile=None, testmode=False,
                 maxdepth=2, maxprocesses=cpu_count(), rsync_args=None):
        self.host = host
        self.port = port
        self.user = user
        self.remote_path = remote_path
        self.local_path = local_path
        self.password = password
        self.password_file = password_file
        self.sshpass_binary_path = spawn.find_executable("sshpass")
        if self.password_file or self.password:
            if self.sshpass_binary_path is None:
                raise ValueError("Make sure to install sshpass")
        self.identityfile = identityfile
        self.testmode = testmode
        self.max_depth = maxdepth
        self.rsync_binary_path = spawn.find_executable("rsync")
        self.ssh_binary_path = spawn.find_executable("ssh")
        self.additional_rsync_args = rsync_args if rsync_args else ""
        self.top_node = Node(self.local_path)
        self.pool = ProcessPool(maxprocesses)

    def initialize_directories(self):
        """
        Transfer directories and files up to a depth of self.depth to local_dir
        Represent the structure in local_dir as a tree of Nodes(),
        with local_dir being the topmost parent in self.top_node
        :return: None
        :rtype: None
        """
        self._transfer_dir(depth=self.max_depth)
        self._map_top_dirs(self.top_node)

    def synchronize_directories(self):
        transfer_function = self._transfer_dir_helper
        return self.pool.start(transfer_function,
                               self._get_lowest_dirs(),
                               self._get_lowest_dirs(remote_dirs=True)
                               )

    def _get_password_from_file(self):
        with open(self.password_file, "r") as pwfile:
            self.password = pwfile.readline()

    def _map_top_dirs(self, parent):
        """
        Create subnodes of self.top_node of all directories done in initial transfer
        :param parent: parent Node with 0-x subdirectories
        :type parent: Node
        :return: None
        :rtype: None
        """
        current_depth = parent.depth
        parent_path = path.abspath(parent.name)
        for subdir in [element for element in listdir(parent_path) if
                       path.isdir(path.join(parent_path, element))]:
            if current_depth == self.max_depth:
                continue
            subdir_node = Node(path.join(parent_path, subdir), parent=parent)
            self._map_top_dirs(subdir_node)

    def _get_lowest_dirs(self,remote_dirs=False):
        """
        Get list of lowest local directory paths (absolute) as mapped by the initial transfer of depth self.max_depth
        :return: list
        :rtype: list of strings
        """
        dir_list = [[node.name for node in children]
                             for children in LevelOrderGroupIter(self.top_node)][-1]
        if remote_dirs:
            dir_list_remote = []
            for dir_path in dir_list:
                absolute_path = dir_path.replace(self.local_path,self.remote_path)
                dir_list_remote.append(absolute_path)
            dir_list = dir_list_remote
        return dir_list

    def _translate_depth_to_string(self, depth):
        """

        :param depth: depth of search for rsync, depth 1 transfers dirs directly below remote_dir
        :type depth: int
        :return: returns string to be used in rsync --exclude parameter
        :rtype:
        """
        logger.info("Translating depth: {}".format(depth))
        rsync_arg = ["/*"]
        for _ in range(depth):
            rsync_arg.append("/*")
        return "".join(rsync_arg)

    def _transfer_dir_helper(self, *args):
        local_path=args[0][0]
        remote_path=args[0][1]
        return self._transfer_dir(local_path=local_path,remote_path=remote_path,depth=None)

    def _transfer_dir(self, local_path=None, remote_path=None, depth=None):
        """
        Do the rsync call. If in testmode, do a local to local transfer only
        :param remote_path: remote path to synchronize, (only subdirs/files below this path are transferred)
        :type remote_path: string
        :param local_path: local path to synchronize to
        :type local_path: string
        :param depth: depth of search for rsync, depth 1 transfers dirs directly below remote_dir
        :type depth: int
        :return:
        :rtype:
        """
        if remote_path is None:
            remote_path = self.remote_path
        if local_path is None:
            local_path = self.local_path

        ssh_command = ""
        if self.identityfile:
            ssh_command = "-e \"{} -i {}\"".format(self.ssh_binary_path, self.identityfile)
        exclude_command = ""
        if depth is not None:
            logger.info("depth is {}".format(depth))
            depth = self._translate_depth_to_string(depth)
            exclude_command = "--exclude=\"{}\"".format(depth)
        sshpass_command = ""
        if self.password or self.password_file:
            sshpass_command = "{} -p {} ".format(self.sshpass_binary_path, self.password)

        if not self.testmode:
            rsync_call = "{sshpass_com} {rsync} {rsync_args} -ar {ssh_com} {exclude_com} {user}@{host}:{remote_path}/ {local_path}".format(
                rsync=self.rsync_binary_path,
                exclude_depth=depth,
                user=self.user,
                host=self.host,
                rsync_args=self.additional_rsync_args,
                sshpass_com=sshpass_command,
                ssh_com=ssh_command,
                exclude_com=exclude_command,
                remote_path=remote_path,
                local_path=local_path)
        else:
            rsync_call = "{rsync} {rsync_args} -ar {exclude_com} {remote_path}/ {local_path}".format(
                rsync=self.rsync_binary_path,
                rsync_args=self.additional_rsync_args,
                exclude_com=exclude_command,
                remote_path=remote_path,
                local_path=local_path)

        logger.info("calling rsync with {}".format(rsync_call).replace(self.password,"XXX"))
        return call(rsync_call, shell=True)



def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--host", "-h", action="store", required=True)
    parser.add_argument("--port", "-p", action="store", type=int, default=22)
    parser.add_argument("--identity-file", "-i", action="store")
    parser.add_argument("--user", "-u", action="store", required=True)
    parser.add_argument("--password", action="store")
    parser.add_argument("--password-file", action="store")
    parser.add_argument("--max-processes", action="store", type=int)
    parser.add_argument("--remote-path", "-r", action="store", required=True)
    parser.add_argument("--local-path", "-l", action="store", required=True)
    parser.add_argument("--max-depth", '-d', action="store", type=int)
    parser.add_argument("--max-threads", "-t", action="store", type=int)
    parser.add_argument("--additional-rsync-args", action="store")
    parser.add_argument("--testmode", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    rcon = RemoteConnect(args.host,
                         args.user,
                         args.remote_path,
                         args.local_path,
                         port=args.port,
                         password=args.password,
                         password_file=args.password_file,
                         identityfile=args.identity_file,
                         maxdepth=args.max_depth,
                         rsync_args=args.additional_rsync_args,
                         testmode=args.testmode)
    rcon.initialize_directories()
    if any(rcon.synchronize_directories()):
        logger.error("ERROR, some rsync call returned bad status.")
    else:
        logger.info("Every rsync run successful.")



if __name__ == '__main__':
    main()
