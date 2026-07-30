import ipaddress
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

TLS_DIR = Path(os.environ.get("BURLA_TLS_DIR", "/var/lib/burla/tls"))
CA_KEY_PATH = TLS_DIR / "cluster-ca.key"
CA_CERT_PATH = TLS_DIR / "cluster-ca.pem"
HEAD_KEY_PATH = TLS_DIR / "head.key"
HEAD_CERT_PATH = TLS_DIR / "head.pem"


def _write_private_key(path: Path, key):
    path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    path.chmod(0o600)


def _new_ca():
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Burla cluster CA")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=5 * 365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=False,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    _write_private_key(CA_KEY_PATH, key)
    CA_CERT_PATH.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))


def _load_ca():
    key = serialization.load_pem_private_key(CA_KEY_PATH.read_bytes(), password=None)
    certificate = x509.load_pem_x509_certificate(CA_CERT_PATH.read_bytes())
    return key, certificate


def _new_leaf(
    common_name: str,
    public_key,
    issuer_key,
    issuer_cert,
    ip_addresses,
    validity_days: int,
    dns_names=(),
):
    now = datetime.now(timezone.utc)
    subject_alt_names = [
        x509.IPAddress(ipaddress.ip_address(address)) for address in ip_addresses
    ]
    subject_alt_names += [x509.DNSName(name) for name in dns_names]
    return (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(issuer_cert.subject)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=validity_days))
        .add_extension(
            x509.SubjectAlternativeName(subject_alt_names),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(public_key),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()),
            critical=False,
        )
        .sign(issuer_key, hashes.SHA256())
    )


def ensure_cluster_tls(head_host: str):
    """`head_host` is the hostname nodes dial the head at: the head VM's
    private IP normally, or a relay hostname in client-hosted mode."""
    try:
        head_ip = ipaddress.ip_address(head_host)
        ip_addresses, dns_names = [head_host], []
    except ValueError:
        head_ip = None
        ip_addresses, dns_names = [], [head_host]

    TLS_DIR.mkdir(parents=True, exist_ok=True)
    regenerated_ca = False
    if not CA_KEY_PATH.exists():
        _new_ca()
        regenerated_ca = True
    else:
        ca_certificate = x509.load_pem_x509_certificate(CA_CERT_PATH.read_bytes())
        try:
            ca_certificate.extensions.get_extension_for_class(x509.SubjectKeyIdentifier)
        except x509.ExtensionNotFound:
            _new_ca()
            regenerated_ca = True

    issuer_key, issuer_cert = _load_ca()
    regenerate_head = regenerated_ca or not HEAD_KEY_PATH.exists()
    if not regenerate_head:
        head_cert = x509.load_pem_x509_certificate(HEAD_CERT_PATH.read_bytes())
        sans = head_cert.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
        if head_ip is not None:
            regenerate_head = head_ip not in sans.get_values_for_type(x509.IPAddress)
        else:
            regenerate_head = head_host not in sans.get_values_for_type(x509.DNSName)
        regenerate_head = regenerate_head or (
            head_cert.not_valid_after_utc
            <= datetime.now(timezone.utc) + timedelta(days=365)
        )

    if regenerate_head:
        head_key = ec.generate_private_key(ec.SECP256R1())
        head_cert = _new_leaf(
            "Burla head",
            head_key.public_key(),
            issuer_key,
            issuer_cert,
            ip_addresses,
            5 * 365,
            dns_names=dns_names,
        )
        _write_private_key(HEAD_KEY_PATH, head_key)
        HEAD_CERT_PATH.write_bytes(head_cert.public_bytes(serialization.Encoding.PEM))


def cluster_ca_pem() -> str:
    return CA_CERT_PATH.read_text()


def sign_node_csr(
    csr_pem: str, public_ip: str, private_ip: str, dns_names=()
) -> str:
    csr = x509.load_pem_x509_csr(csr_pem.encode())
    if not csr.is_signature_valid:
        raise ValueError("Invalid certificate signing request")
    issuer_key, issuer_cert = _load_ca()
    certificate = _new_leaf(
        "Burla node",
        csr.public_key(),
        issuer_key,
        issuer_cert,
        [public_ip, private_ip],
        7,
        dns_names=dns_names,
    )
    return certificate.public_bytes(serialization.Encoding.PEM).decode()
