package secrets

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

// AzureConfig configures Azure Key Vault. Credentials come from the SDK's
// default chain: on AKS, workload identity (pods labelled
// azure.workload.identity/use=true, the service account annotated with
// azure.workload.identity/client-id); on Azure VMs, a managed identity;
// elsewhere AZURE_* variables.
type AzureConfig struct {
	// VaultURL is the vault, e.g. https://acme-turgon.vault.azure.net.
	VaultURL string
	// Prefix is put before every secret name, e.g. "turgon-".
	Prefix string
	TTL    time.Duration
	// Credential and Transport, if set, replace the default chain and HTTP
	// client (tests).
	Credential azcore.TokenCredential
	Transport  policy.Transporter
	// InsecureChallenge accepts a token challenge from a server outside
	// the vault's domain (a test server only).
	InsecureChallenge bool
	// APIVersion pins the Key Vault API version (default: the SDK's), for
	// clouds that lag behind, such as Azure Stack Hub.
	APIVersion string
}

type azureFetcher struct {
	client *azsecrets.Client
	vault  string
}

// NewAzure returns a resolver for Azure Key Vault.
func NewAzure(c AzureConfig) (*Cloud, error) {
	u, err := url.Parse(c.VaultURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, errors.New("azure: the vault URL must be https://<vault>.vault.azure.net (TURGON_AZURE_VAULT_URL)")
	}
	cred := c.Credential
	if cred == nil {
		cred, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("azure: %w", err)
		}
	}
	// The SDK checks that the token a vault asks for is for the vault's own
	// domain; an emulator on this host (Lowkey Vault) is not under
	// vault.azure.net.
	opts := &azsecrets.ClientOptions{DisableChallengeResourceVerification: c.InsecureChallenge || loopback(u.Hostname())}
	if c.Transport != nil {
		opts.Transport = c.Transport
	}
	opts.APIVersion = c.APIVersion
	client, err := azsecrets.NewClient(c.VaultURL, cred, opts)
	if err != nil {
		return nil, fmt.Errorf("azure: %w", err)
	}
	return newCloud(&azureFetcher{client: client, vault: u.Host}, c.Prefix, c.TTL), nil
}

func (*azureFetcher) kind() string { return "Azure Key Vault" }

// Key Vault names allow letters, digits and dashes only.
var (
	azureNameRE   = regexp.MustCompile(`^[0-9A-Za-z-]{1,127}$`)
	azureReplacer = strings.NewReplacer("/", "--", "_", "-", ".", "-")
)

func (*azureFetcher) secretName(path string) (string, error) {
	return mapName("Azure Key Vault", path, azureReplacer, azureNameRE)
}

func (a *azureFetcher) fetch(ctx context.Context, name string) (string, error) {
	resp, err := a.client.GetSecret(ctx, name, "", nil)
	if err != nil {
		var re *azcore.ResponseError
		if errors.As(err, &re) {
			return "", fmt.Errorf("azure key vault %s: %d %s", name, re.StatusCode, re.ErrorCode)
		}
		return "", fmt.Errorf("azure key vault %s: %w", name, err)
	}
	if resp.Value == nil {
		return "", fmt.Errorf("azure key vault %s: no value", name)
	}
	return *resp.Value, nil
}

func (a *azureFetcher) fix(err error, name string) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, fmt.Sprintf(" %d ", http.StatusNotFound)) || strings.Contains(msg, "SecretNotFound"):
		return fmt.Sprintf("Create it: az keyvault secret set --vault-name %s --name %s --value '{\"<field>\": \"...\"}'.", strings.Split(a.vault, ".")[0], name)
	case strings.Contains(msg, fmt.Sprintf(" %d ", http.StatusForbidden)):
		return "Give Turgon's identity the Key Vault Secrets User role on the vault (or get on secrets in its access policy), and check the vault's firewall."
	case strings.Contains(msg, "credential") || strings.Contains(msg, "authentication"):
		return "No Azure identity was found. On AKS, label Turgon's pods azure.workload.identity/use=true and annotate its service account with azure.workload.identity/client-id; on a VM, assign a managed identity."
	}
	return ""
}
