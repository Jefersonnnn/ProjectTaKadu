import os
from ftplib import FTP, all_errors

import paramiko


class ClientFTP:
    def __init__(self, user: str, password: str, host: str, port: int = 21, default_path: str = '', tls_ssl=False):
        """
        Reponsável por ler e gravar arquivos no FTP.

        :param host: Endereço do FTP
        :param user: Usuário autenticado para leitura e escrita
        :param password: Senha do usuário
        """
        self.tls_ssl = tls_ssl
        self.host = host
        self.port = port
        self.default_path = default_path
        self.user = user
        self.password = password

        self._ftp = None
        self._ssh = None
        self.conn_status = None
        self.pending_processing = False

    def connect(self):
        """
        Conecta com o servidor FTP.
        """
        try:
            if self.tls_ssl:
                # Cria uma instância do cliente SSH
                self._ssh = paramiko.SSHClient()
                # Configura a politica de verificação do host
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                # Conecta ao servidor SFTP
                self._ssh.connect(self.host, self.port, self.user, self.password)
                self._ftp = self._ssh.open_sftp()
            else:
                self._ftp = FTP(self.host)
                self.conn_status = self._ftp.login(self.user, self.password).split(None, 1)[0]
            return self
        except all_errors as e:
            self.conn_status = str(e).split(None, 1)[0]
            print('erro: ', e)
            return None

    def chdir(self, directory: str):
        """
        Navega até o diretório "dir".

        :param directory: Caminho relativo do diretório.
        """
        if not self._ftp:
            print("Connect first")
            return

        try:
            if self.tls_ssl:
                self._ftp.chdir(directory)
            else:
                self._ftp.cwd(directory)

        except all_errors as e:
            print('erro', e)

    def list_files(self, file_type_filter: str = None) -> [str]:
        """
        Lista os arquivos no diretório atual baseado ou não em um filtro de tipo do arquivo.

        :param file_type_filter: Extensão dos arquivos que devem ser retornados.
        :return: Lista com o nome dos arquivos encontrados.
        :rtype: [str]
        """
        if not self._ftp:
            print("Connect first")
            return []

        try:
            if self.tls_ssl:
                files = self._ftp.listdir()
            else:
                files = self._ftp.nlst()
            if not files:
                return []

            if not file_type_filter:
                return files

            found_files = [file for file in files if file.endswith(file_type_filter)]
            return found_files

        except all_errors as e:
            print('erro', e)
            return []

    def download(self, file: str, download_path: str = ''):
        """
        Faz o download do arquivo no diretório atual.

        :param file: Nome do arquivo para download.
        :param download_path: Diretório destino.
        """
        if not self._ftp:
            print("Connect first")
            return False

        try:
            self.pending_processing = True
            if self.tls_ssl:
                local_file_path = os.path.join(download_path, file)
                remote_file_path = file
                self._ftp.get(remote_file_path, local_file_path)
            else:
                with open(os.path.join(download_path, file), 'wb') as downloaded_file:
                    self._ftp.retrbinary(f'RETR {file}', downloaded_file.write)
            self.pending_processing = False
            print(f'Downloaded {file}')
            return True

        except paramiko.SSHException as e:
            print('erro: ', e)
            return False
        except all_errors as e:
            print('erro: ', e)
            return False

    def create_remote_directory_if_not_exists(self, directory_path: str):
        """
        Verifica se o diretório remoto existe e, caso não exista, cria-o.
        :param directory_path: Caminho do diretório remoto.
        """
        try:
            # Verifica se o diretório já existe
            if self._ftp.stat(directory_path):
                return
        except FileNotFoundError:
            # Diretório não encontrado, então cria
            try:
                self._ftp.mkdir(directory_path)
                print(f"O diretório {directory_path} foi criado com sucesso.")
            except Exception as e:
                print(f"Erro ao criar o diretório {directory_path}: {e}")
        except Exception as e:
            print(f"Erro ao verificar o diretório {directory_path}: {e}")

    def upload(self, file: str, file_path: str = '', dest_path: str = ''):
        """
        Faz o upload do arquivo no diretório atual.

        :param file: Nome do arquivo para upload.
        :param file_path: Diretório do arquivo.
        :param dest_path: Diretório no FTP para salvar o arquivo
        """
        if not self._ftp:
            print("Connect first")
            return

        try:
            self.pending_processing = True

            if self.tls_ssl:
                local_file_path = os.path.join(file_path)
                remote_file_path = dest_path + '/' + file
                self.create_remote_directory_if_not_exists(dest_path)
                self._ftp.put(local_file_path, remote_file_path)
            else:
                with open(os.path.join(file_path, file), 'rb') as file_object:
                    self._ftp.storbinary(f'STOR {file}', file_object)
            self.pending_processing = False

        except all_errors as e:
            print('erro', e)

    def check_conn(self):
        """
        Verifica se a conexão com o servidor FTP está ativa.
        """
        if not self._ftp:
            print("Connect first")
            return False

        try:
            return self._ftp.pwd() is not None

        except all_errors as e:
            print('erro', e)
            return False

    def quit(self):
        """
        Encerra a conexão atual.
        """
        if not self._ftp:
            print("Connect first")
            return

        while self.pending_processing:
            pass

        try:
            if self._ftp:
                if self.tls_ssl:
                    self._ftp.close()
                else:
                    self._ftp.quit()
                self._ftp = None

            if self._ssh:
                self._ssh.close()
                self._ssh = None

        except all_errors as e:
            print('erro', e)
