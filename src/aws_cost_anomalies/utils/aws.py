"""Shared AWS helpers."""

from __future__ import annotations

import boto3


def aws_session(profile: str = "") -> boto3.Session:
    """Create a boto3 Session, optionally with a named profile.

    When *profile* is empty the default credential chain is used,
    identical to calling ``boto3.Session()`` with no arguments.

    Note: ``boto3.Session()`` does **not** raise on construction for
    bad credentials — errors surface on the first API call.  A non-existent
    profile name will raise ``botocore.exceptions.ProfileNotFound`` here.
    """
    if profile:
        return boto3.Session(profile_name=profile)
    return boto3.Session()
