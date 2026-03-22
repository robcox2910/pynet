"""Sending packages -- file transfer protocol.

Model FTP concepts without making real network calls. FTP (File
Transfer Protocol) is one of the oldest ways to move files between
computers. Think of it as a package delivery service: you log in
(USER/PASS), browse the warehouse (LIST), grab a package (RETR),
drop one off (STOR), and leave (QUIT).

FTP commands are plain-text lines, each ending with a carriage
return and line feed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FTPCommand:
    """Represent a single FTP command.

    Attributes:
        command: The FTP verb (USER, PASS, LIST, RETR, STOR, QUIT).
        argument: Optional argument for the command.

    """

    command: str
    argument: str = ""

    def format(self) -> str:
        """Format this command as an FTP protocol line.

        Returns:
            The command string with optional argument.

        """
        if self.argument:
            return f"{self.command} {self.argument}"
        return self.command


def build_ftp_session(commands: list[FTPCommand]) -> str:
    """Format a list of FTP commands as protocol text.

    Assemble the conversation a client has with an FTP server,
    like writing out the steps for a package pickup.

    Args:
        commands: The FTP commands to format in order.

    Returns:
        The full session as a CRLF-delimited string.

    """
    return "\r\n".join(cmd.format() for cmd in commands)
