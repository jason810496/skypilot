"""Unit tests for the `sky rsync` CLI command."""
from unittest import mock

from click import testing as cli_testing
import pytest

from sky.client.cli import command


class TestExtractClusterFromRsyncArgs:
    """Tests for _extract_cluster_from_rsync_args helper."""

    def test_source_has_cluster(self):
        result = command._extract_cluster_from_rsync_args(
            'my-cluster:~/file.txt', './')
        assert result == 'my-cluster'

    def test_destination_has_cluster(self):
        result = command._extract_cluster_from_rsync_args(
            './local/', 'my-cluster:~/remote/')
        assert result == 'my-cluster'

    def test_no_cluster(self):
        result = command._extract_cluster_from_rsync_args(
            './local/', './other/')
        assert result is None

    def test_both_have_cluster(self):
        from click import UsageError
        with pytest.raises(UsageError, match='Remote-to-remote'):
            command._extract_cluster_from_rsync_args('cluster-a:~/src',
                                                     'cluster-b:~/dst')

    def test_cluster_name_with_colon_path(self):
        result = command._extract_cluster_from_rsync_args(
            'my-cluster:/home/user/data', './')
        assert result == 'my-cluster'


class TestRsyncCommand:
    """Tests for the sky rsync CLI command."""

    def test_no_cluster_prefix_errors(self):
        """Should error when neither arg has a cluster prefix."""
        runner = cli_testing.CliRunner()
        result = runner.invoke(command.rsync, ['./local/', './other/'])
        assert result.exit_code != 0
        assert 'must contain a cluster prefix' in result.output

    def test_cluster_not_found(self, monkeypatch):
        """Should error when cluster is not found."""
        monkeypatch.setattr(
            'sky.client.cli.command._get_cluster_records_and_set_ssh_config',
            lambda **kwargs: [])
        runner = cli_testing.CliRunner()
        result = runner.invoke(command.rsync, ['my-cluster:~/file.txt', './'])
        assert result.exit_code != 0
        assert 'not found' in result.output

    def test_cluster_not_up(self, monkeypatch):
        """Should error when cluster handle has no IPs (not UP)."""
        mock_handle = mock.MagicMock()
        mock_handle.cached_external_ips = None
        monkeypatch.setattr(
            'sky.client.cli.command._get_cluster_records_and_set_ssh_config',
            lambda **kwargs: [{
                'handle': mock_handle,
                'name': 'my-cluster'
            }])
        runner = cli_testing.CliRunner()
        result = runner.invoke(command.rsync, ['my-cluster:~/file.txt', './'])
        assert result.exit_code != 0
        assert 'not UP' in result.output

    def test_successful_rsync_download(self, monkeypatch):
        """Should call rsync with correct arguments for download."""
        mock_handle = mock.MagicMock()
        mock_handle.cached_external_ips = ['1.2.3.4']
        monkeypatch.setattr(
            'sky.client.cli.command._get_cluster_records_and_set_ssh_config',
            lambda **kwargs: [{
                'handle': mock_handle,
                'name': 'my-cluster'
            }])

        mock_run = mock.MagicMock(return_value=mock.MagicMock(returncode=0))
        monkeypatch.setattr('subprocess.run', mock_run)

        runner = cli_testing.CliRunner()
        result = runner.invoke(command.rsync, ['my-cluster:~/file.txt', './'])
        assert result.exit_code == 0
        mock_run.assert_called_once_with(
            ['rsync', 'my-cluster:~/file.txt', './'], check=True)

    def test_successful_rsync_upload(self, monkeypatch):
        """Should call rsync with correct arguments for upload."""
        mock_handle = mock.MagicMock()
        mock_handle.cached_external_ips = ['1.2.3.4']
        monkeypatch.setattr(
            'sky.client.cli.command._get_cluster_records_and_set_ssh_config',
            lambda **kwargs: [{
                'handle': mock_handle,
                'name': 'my-cluster'
            }])

        mock_run = mock.MagicMock(return_value=mock.MagicMock(returncode=0))
        monkeypatch.setattr('subprocess.run', mock_run)

        runner = cli_testing.CliRunner()
        result = runner.invoke(command.rsync,
                               ['./local/dir/', 'my-cluster:~/remote/'])
        assert result.exit_code == 0
        mock_run.assert_called_once_with(
            ['rsync', './local/dir/', 'my-cluster:~/remote/'], check=True)

    def test_extra_rsync_args(self, monkeypatch):
        """Extra args after -- should be passed to rsync."""
        mock_handle = mock.MagicMock()
        mock_handle.cached_external_ips = ['1.2.3.4']
        monkeypatch.setattr(
            'sky.client.cli.command._get_cluster_records_and_set_ssh_config',
            lambda **kwargs: [{
                'handle': mock_handle,
                'name': 'my-cluster'
            }])

        mock_run = mock.MagicMock(return_value=mock.MagicMock(returncode=0))
        monkeypatch.setattr('subprocess.run', mock_run)

        runner = cli_testing.CliRunner()
        result = runner.invoke(
            command.rsync,
            ['my-cluster:~/data/', './data/', '--', '-avz', '--progress'])
        assert result.exit_code == 0
        mock_run.assert_called_once_with(
            ['rsync', '-avz', '--progress', 'my-cluster:~/data/', './data/'],
            check=True)

    def test_rsync_not_installed(self, monkeypatch):
        """Should show a friendly error if rsync is not installed."""
        mock_handle = mock.MagicMock()
        mock_handle.cached_external_ips = ['1.2.3.4']
        monkeypatch.setattr(
            'sky.client.cli.command._get_cluster_records_and_set_ssh_config',
            lambda **kwargs: [{
                'handle': mock_handle,
                'name': 'my-cluster'
            }])

        monkeypatch.setattr('subprocess.run',
                            mock.MagicMock(side_effect=FileNotFoundError))

        runner = cli_testing.CliRunner()
        result = runner.invoke(command.rsync, ['my-cluster:~/file.txt', './'])
        assert result.exit_code != 0
        assert 'rsync is not installed' in result.output

    def test_both_remote_errors(self):
        """Should error when both source and dest are remote."""
        runner = cli_testing.CliRunner()
        result = runner.invoke(command.rsync,
                               ['cluster-a:~/src', 'cluster-b:~/dst'])
        assert result.exit_code != 0
        assert 'Remote-to-remote' in result.output
