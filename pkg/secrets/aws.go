package secrets

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"
)

// AWSConfig configures AWS Secrets Manager. Credentials come from the
// SDK's default chain: on EKS, IRSA (a service account annotated with
// eks.amazonaws.com/role-arn) or EKS Pod Identity; on EC2, the instance
// role; elsewhere AWS_* variables or a shared profile.
type AWSConfig struct {
	// Region, e.g. eu-central-1 (default: the SDK's, AWS_REGION).
	Region string
	// Prefix is put before every secret name, e.g. "turgon/".
	Prefix string
	// Endpoint overrides the service endpoint (a VPC endpoint, or a test
	// server); AWS_ENDPOINT_URL_SECRETS_MANAGER works too.
	Endpoint string
	TTL      time.Duration
	// Credentials, if set, replace the default chain (tests).
	Credentials aws.CredentialsProvider
}

type awsFetcher struct {
	client *secretsmanager.Client
	region string
}

// NewAWS returns a resolver for AWS Secrets Manager.
func NewAWS(ctx context.Context, c AWSConfig) (*Cloud, error) {
	var opts []func(*config.LoadOptions) error
	if c.Region != "" {
		opts = append(opts, config.WithRegion(c.Region))
	}
	if c.Credentials != nil {
		opts = append(opts, config.WithCredentialsProvider(c.Credentials))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws: %w", err)
	}
	if cfg.Region == "" {
		return nil, errors.New("aws: no region: set TURGON_AWS_REGION (or AWS_REGION)")
	}
	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
	return newCloud(&awsFetcher{client: client, region: cfg.Region}, c.Prefix, c.TTL), nil
}

func (*awsFetcher) kind() string { return "AWS Secrets Manager" }

var awsNameRE = regexp.MustCompile(`^[A-Za-z0-9/_+=.@-]{1,512}$`)

func (*awsFetcher) secretName(path string) (string, error) {
	return mapName("AWS Secrets Manager", path, nil, awsNameRE)
}

func (a *awsFetcher) fetch(ctx context.Context, name string) (string, error) {
	out, err := a.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(name)})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) {
			return "", fmt.Errorf("aws secrets manager %s: %s: %s", name, api.ErrorCode(), api.ErrorMessage())
		}
		return "", fmt.Errorf("aws secrets manager %s: %w", name, err)
	}
	if out.SecretString != nil {
		return *out.SecretString, nil
	}
	return string(out.SecretBinary), nil
}

func (a *awsFetcher) fix(err error, name string) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "ResourceNotFoundException"):
		return fmt.Sprintf("Create it in %s: aws secretsmanager create-secret --name %s --secret-string '{\"<field>\": \"...\"}'.", a.region, name)
	case strings.Contains(msg, "AccessDeniedException"):
		return fmt.Sprintf("The role Turgon runs as needs secretsmanager:GetSecretValue on arn:aws:secretsmanager:%s:<account>:secret:%s-* (and kms:Decrypt on the secret's key, if it has its own).", a.region, name)
	case strings.Contains(msg, "credentials"):
		return "No AWS credentials were found. On EKS, annotate Turgon's service account with eks.amazonaws.com/role-arn (IRSA) or add an EKS Pod Identity association; elsewhere use an instance role or AWS_* variables."
	case strings.Contains(msg, "DecryptionFailure"):
		return "Secrets Manager could not decrypt the secret: give the role kms:Decrypt on the secret's KMS key."
	}
	return ""
}
