import configparser
from paramiko.client import SSHClient
import sys
import os
import datetime
import tarfile
import gzip
import pymysql

class SiteCopy:
    """ Разворачивает сайт с боевого сервера в локальном окружении

    Решаемая проблема:
    При работе над сайтом каждый раз приходится вручную скачивать
    и разворачивать актуальную версию сайта.

    @author: Dmitry Demidov <mail@demidov.media>
    @date: 23.02.2016
    """

    def __init__(self, argv=[]):
        # Устанавливаем значения по умолчанию
        self.config_file = 'config.ini'
        self.app_path = os.path.realpath(__file__)
        self.filename = None
        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.ssh = None
        self.db = None
        self.sftp = None

        # Обрабатываем входные параметры
        if len(argv) > 0:
            self.process_argv(argv)

        # Получаем конфигурацию
        config_path = self.config_file
        if not os.path.exists(config_path):
            config_path = os.path.join(self.app_path, self.config_file)
        if not os.path.isfile(config_path):
            SiteCopy.print_usage()
            self.end()
        else:
            self.config = configparser.ConfigParser()
            self.config.read(self.config_file)

        # Берем значения из конфига
        self.files_config = {
            'files_path': self.config.get('Files Configuration', 'files_path'),
            'archives_path': self.config.get('Files Configuration', 'archives_path'),
            'filemask': self.config.get('Files Configuration', 'filemask'),
            'local_files_path': self.config.get('Files Configuration', 'local_files_path'),
            'local_tmp_path': self.config.get('Files Configuration', 'local_tmp_path'),
        }
        self.ssh_config = {
            'username': self.config.get('SSHConnection', 'username'),
            'password': self.config.get('SSHConnection', 'password'),
            'hostname': self.config.get('SSHConnection', 'hostname'),
            'port': self.config.getint('SSHConnection', 'port'),
        }
        self.mysql_config = {
            'host': self.config.get('MYSQL Remote', 'host'),
            'port': self.config.getint('MYSQL Remote', 'port'),
            'user': self.config.get('MYSQL Remote', 'user'),
            'password': self.config.get('MYSQL Remote', 'password'),
            'dbname': self.config.get('MYSQL Remote', 'dbname'),
        }
        self.mysql_local_config = {
            'host': self.config.get('MYSQL Local', 'host'),
            'port': self.config.getint('MYSQL Local', 'port'),
            'user': self.config.get('MYSQL Local', 'user'),
            'password': self.config.get('MYSQL Local', 'password'),
            'dbname': self.config.get('MYSQL Local', 'dbname'),
        }
        # Создаем каталоги из конфигурации
        if not os.path.exists(self.files_config['local_files_path']):
            os.makedirs(self.files_config['local_files_path'])
        if not os.path.exists(self.files_config['local_tmp_path']):
            os.makedirs(self.files_config['local_tmp_path'])
        print("SiteCopy greetings you!")
        print("Let's try to connect to SSH")
        self.ssh_connect()
        self.mysql_connect()
        # Генерируем имена файлов
        self.archivefile = "{}.tar.gz".format(self.get_file_name())
        self.dbfile = "{}.sql.gz".format(self.get_file_name())

    def process_argv(self, argv):
        """
        Обрабатываем параметры командной строки

        :param argv: Результат sys.argv
        :return:
        """
        for key, param in enumerate(argv):
            if key == 0:
                continue
            if param == '-c' or param == '--config':
                try:
                    self.config_file = argv[key + 1]
                except IndexError:
                    SiteCopy.print_usage()
                    self.end()
            elif param == '-h' or param == '--help':
                SiteCopy.print_usage()
            elif param == '-v' or param == '--version':
                SiteCopy.version()

    def run(self):
        print("Packing files...")
        self.pack_files()
        print("Begin DB pack")
        self.pack_db()
        self.transfer()
        self.extract_local_files()
        self.dbimport()
        self.clear_all()
        self.end()

    @classmethod
    def print_usage(cls):
        print("Usage: python3 sitecopy.py -c <configfile> -h -v")

    @classmethod
    def version(cls):
        print("SiteCopy Version: 0.0.1 by Dmitry Demidov <mail@demidov.media>")

    def end(self):
        """ Завершаем работу
        :return:
        """
        self.stdin.close()
        self.stdout.close()
        self.stderr.close()
        self.ssh.close()  # Закрываем коннект к ssh
        self.db.close()
        exit()

    def __print_config(self):
        for section, section_proxy in self.config.items():
            print("Section: {} | Section Proxy: {}".format(section, section_proxy))
            for key, value in self.config.items(section):
                print("{}: {}".format(key, value))

    def ssh_connect(self):
        """ Отвечает за коннект к серверу
        :return:
        """
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(
            self.ssh_config['hostname'],
            port=self.ssh_config['port'],
            username=self.ssh_config['username'],
            password=self.ssh_config['password']
        )
        self.sftp = self.ssh.open_sftp()

    def mysql_connect(self):
        self.db = pymysql.connect(
            user=self.mysql_local_config['user'],
            password=self.mysql_local_config['password'],
            charset='utf8mb4',
        )
        # Drop database
        with self.db.cursor() as cursor:
            sql = "DROP DATABASE IF EXISTS {}".format(self.mysql_local_config['dbname'])
            cursor.execute(sql)

        # Recreate database
        with self.db.cursor() as cursor:
            sql = "CREATE DATABASE {} CHARACTER SET utf8 COLLATE utf8_general_ci".format(self.mysql_local_config['dbname'])
            cursor.execute(sql)

        # Select DATABASE
        with self.db.cursor() as cursor:
            sql = "USE {}".format(self.mysql_local_config['dbname'])
            cursor.execute(sql)


    def pack_files(self):
        """
        Делаем резервную копию файлов удаленного сайта
        :return:
        """
        self.exec("tar czf {0}/{1} -C {2} .".format(self.files_config['archives_path'], self.archivefile, self.files_config['files_path']))
        self.stdout.read()
        print("All files are packed in")

    def pack_db(self):
        # Генерируем имя файла
        self.exec("mysqldump -u{} -h{} -P{} -p{} {} | gzip > {}".format(
            self.mysql_config['user'],
            self.mysql_config['host'],
            self.mysql_config['port'],
            self.mysql_config['password'],
            self.mysql_config['dbname'],
            "{0}/{1}".format(self.files_config['archives_path'], self.dbfile)
        ))
        self.stdout.read()

    def transfer(self):
        print("Transfer files begin")
        self.sftp.get("{}/{}".format(self.files_config['archives_path'], self.archivefile), os.path.join(self.files_config['local_tmp_path'], self.archivefile))
        self.sftp.get("{}/{}".format(self.files_config['archives_path'], self.dbfile), os.path.join(self.files_config['local_tmp_path'], self.dbfile))
        print("Endof transfer files")

    def clear_all(self):
        print("Clearing remote archives")
        self.exec("rm {0}/{1} {0}/{2}".format(self.files_config['archives_path'], self.archivefile, self.dbfile))
        self.stdout.read()
        print("Clearing local archives")
        if os.path.isfile(os.path.join(self.files_config['local_tmp_path'], 'db.sql')):
            os.unlink(os.path.join(self.files_config['local_tmp_path'], 'db.sql'))
            os.unlink(os.path.join(self.files_config['local_tmp_path'], self.archivefile))
            os.unlink(os.path.join(self.files_config['local_tmp_path'], self.dbfile))

    def extract_local_files(self):
        tar = tarfile.open(os.path.join(self.files_config['local_tmp_path'], self.archivefile))
        tar.extractall(self.files_config['local_files_path'])
        tar.close()
        with gzip.open(os.path.join(self.files_config['local_tmp_path'], self.dbfile), 'rb') as infile:
            with open(os.path.join(self.files_config['local_tmp_path'], "db.sql"), 'wb') as outfile:
                for line in infile:
                    outfile.write(line)

    def dbimport(self):
        # import database
        with open(os.path.join(self.files_config['local_tmp_path'], "db.sql"), 'r') as dumpfile:
            with self.db.cursor() as cursor:
                sql = " ".join(dumpfile.readlines())
                cursor.execute(sql)
        self.db.commit()

    def get_file_name(self):
        """
        Генерируем имя файла и возвращаем его. Один раз за сеанс

        :return: string имя файла
        """
        if self.filename is None:
            now = datetime.datetime.now()
            fileindex = 1
            # Генерируем имя файла
            filename = '{0}-{1}'.format(now.strftime('%Y%m%d'), self.files_config['filemask'])
            # Проверочный файл (проверяем, делали уже такую копию)
            testfile = "{0}/{1}.{2}".format(self.files_config['archives_path'], filename, 'tar.gz')
            # Проверяем наличие файла, если уже делали такую копию, меняем имя
            self.exec("ls {0}".format(testfile))
            while testfile in str(self.stdout.read()):
                print("File exists, try next index: {0}".format(fileindex))
                filename = '{0}-{1}-{2}'.format(now.strftime('%Y%m%d'), self.files_config['filemask'], fileindex)
                testfile = "{0}/{1}.{2}".format(self.files_config['archives_path'], filename, 'tar.gz')
                self.exec("ls {0}".format(testfile))
                fileindex += 1
            self.filename = filename
        return self.filename

    def exec(self, command):
        self.stdin, self.stdout, self.stderr = self.ssh.exec_command(command)

if __name__ == '__main__':
    # Вызываем основной класс и передаем параметры из командной строки
    app = SiteCopy(sys.argv)
    app.run()
