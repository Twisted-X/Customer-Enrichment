"""
Paramiko SFTP: public-key auth, then password via ssh-userauth, then keyboard-interactive.
SFTPGo often accepts the same password only through keyboard-interactive (OpenSSH does this
transparently; Paramiko's auth_password alone may fail).
"""
from __future__ import annotations

import socket


def _load_private_key(path: str):
    import paramiko

    for key_cls in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
        try:
            return key_cls.from_private_key_file(path)
        except Exception:
            continue
    return None


def open_sftp(
    host: str,
    port: int,
    username: str,
    key_path: str = "",
    password: str = "",
    *,
    timeout: float = 15.0,
):
    """
    Return an authenticated paramiko.SFTPClient.
    Caller should call .close(); then close the transport via .get_transport().close().
    """
    import paramiko

    sock = socket.create_connection((host, port), timeout=timeout)
    transport = paramiko.Transport(sock)
    transport.start_client(timeout=timeout)

    key_path = (key_path or "").strip()
    password = password or ""

    if key_path:
        pkey = _load_private_key(key_path)
        if pkey:
            try:
                transport.auth_publickey(username, pkey)
            except paramiko.AuthenticationException:
                pass

    if not transport.is_authenticated() and password:
        try:
            transport.auth_password(username, password)
        except paramiko.AuthenticationException:
            pass

    if not transport.is_authenticated() and password:

        def _handler(title, instructions, prompt_list):
            if not prompt_list:
                return []
            return [password for _ in prompt_list]

        transport.auth_interactive(username, _handler)

    if not transport.is_authenticated():
        transport.close()
        raise paramiko.AuthenticationException(
            "SFTP authentication failed (user, password, or public key)"
        )

    return paramiko.SFTPClient.from_transport(transport)


def close_sftp(sftp) -> None:
    """Close SFTP session and underlying SSH transport (Paramiko has no SFTPClient.get_transport)."""
    try:
        ch = sftp.get_channel()
        tr = ch.get_transport() if ch is not None else None
        sftp.close()
        if tr is not None:
            tr.close()
    except Exception:
        pass
