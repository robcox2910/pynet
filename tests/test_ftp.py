"""Test FTP command modelling."""

from pynet.ftp import FTPCommand, build_ftp_session


class TestFTPCommand:
    """Test the FTPCommand dataclass."""

    def test_command_with_argument(self) -> None:
        """Format a command that has an argument."""
        cmd = FTPCommand(command="USER", argument="alice")

        assert cmd.format() == "USER alice"

    def test_command_without_argument(self) -> None:
        """Format a command that has no argument."""
        cmd = FTPCommand(command="QUIT")

        assert cmd.format() == "QUIT"

    def test_default_argument_is_empty(self) -> None:
        """Default the argument to an empty string."""
        cmd = FTPCommand(command="LIST")

        assert cmd.argument == ""

    def test_retr_command(self) -> None:
        """Format a RETR command with a filename argument."""
        cmd = FTPCommand(command="RETR", argument="readme.txt")

        assert cmd.format() == "RETR readme.txt"

    def test_stor_command(self) -> None:
        """Format a STOR command with a filename argument."""
        cmd = FTPCommand(command="STOR", argument="upload.bin")

        assert cmd.format() == "STOR upload.bin"


class TestBuildFTPSession:
    """Test the FTP session builder."""

    def test_single_command(self) -> None:
        """Format a single-command session."""
        session = build_ftp_session([FTPCommand(command="QUIT")])

        assert session == "QUIT"

    def test_full_session(self) -> None:
        """Format a complete login-list-quit session."""
        commands = [
            FTPCommand(command="USER", argument="alice"),
            FTPCommand(command="PASS", argument="secret"),
            FTPCommand(command="LIST"),
            FTPCommand(command="QUIT"),
        ]
        session = build_ftp_session(commands)

        assert session == "USER alice\r\nPASS secret\r\nLIST\r\nQUIT"

    def test_empty_session(self) -> None:
        """Return an empty string for an empty command list."""
        session = build_ftp_session([])

        assert session == ""

    def test_commands_separated_by_crlf(self) -> None:
        """Separate each command with CRLF."""
        commands = [
            FTPCommand(command="USER", argument="bob"),
            FTPCommand(command="QUIT"),
        ]
        session = build_ftp_session(commands)

        assert "\r\n" in session

    def test_session_preserves_order(self) -> None:
        """Preserve command order in the output."""
        commands = [
            FTPCommand(command="USER", argument="alice"),
            FTPCommand(command="RETR", argument="file.txt"),
            FTPCommand(command="QUIT"),
        ]
        session = build_ftp_session(commands)
        lines = session.split("\r\n")

        assert lines[0] == "USER alice"
        assert lines[1] == "RETR file.txt"
        assert lines[2] == "QUIT"
