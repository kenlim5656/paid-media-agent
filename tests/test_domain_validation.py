# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""Domain allowlist validation (tools/crm_client._validate_domain)."""
import pytest

from tools.crm_client import _validate_domain


@pytest.mark.parametrize("domain,expected", [
    ("acme.com", "acme.com"),
    ("ACME.COM", "acme.com"),                  # normalized to lowercase
    ("  acme.com  ", "acme.com"),              # trimmed
    ("sub.domain.acme-corp.io", "sub.domain.acme-corp.io"),
    ("a1.co", "a1.co"),
])
def test_valid_domains(domain, expected):
    assert _validate_domain(domain) == expected


@pytest.mark.parametrize("domain", [
    "",                                         # empty
    "   ",                                      # whitespace only
    "https://acme.com",                         # scheme
    "acme.com/path",                            # path
    "acme.com' OR '1'='1",                      # SQL injection attempt
    "acme.com; DROP TABLE crm_leads_staging;",  # SQL injection attempt
    "-acme.com",                                # leading hyphen
    "acme.com-",                                # trailing hyphen
    "acme com",                                 # space
    "a" * 300 + ".com",                         # too long
])
def test_invalid_domains_rejected(domain):
    with pytest.raises(ValueError):
        _validate_domain(domain)
