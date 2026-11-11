package xray

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// AccessGrants names, by principal, who may sign in and who gets the admin
// interface (an MCP token, voice writes), for every sign-in provider. They come
// from the deployment's configuration (--access-users, --access-admins), never
// from code. ADR-066.
//
// A principal belongs to one provider and never matches a session of another:
// an Entra UPN can look like a Google address.
type AccessGrants struct {
	users  []string // normalised principals; empty with no admins = whoever the provider admits
	admins []string // normalised principals; may sign in and get the admin interface
}

// grants is process-wide, like the session store; main sets it once at startup.
var grants AccessGrants

// SetAccessGrants installs the grants. Call before serving.
func SetAccessGrants(g AccessGrants) { grants = g }

const principalForms = "google:<address>, github:<numeric user id>, azure:<object id>"

var azureOID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// NewAccessGrants parses comma-separated principal lists. A google address must
// be in the Workspace domain (--google-domain): sign-in then admits only that
// domain's managed accounts (the hd claim), whereas a consumer Google account can
// carry a verified address on any domain and would match a list without that
// check. GitHub and Entra principals are the numeric id and the object id, the
// same ids the session carries: a login or UPN can be renamed and re-registered.
func NewAccessGrants(users, admins, domain string) (AccessGrants, error) {
	domain = strings.ToLower(strings.TrimSpace(domain))
	u, err := parsePrincipals(users, domain)
	if err != nil {
		return AccessGrants{}, fmt.Errorf("access users: %w", err)
	}
	a, err := parsePrincipals(admins, domain)
	if err != nil {
		return AccessGrants{}, fmt.Errorf("access admins: %w", err)
	}
	return AccessGrants{users: u, admins: a}, nil
}

func parsePrincipals(list, domain string) ([]string, error) {
	var out []string
	for _, e := range strings.Split(list, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		p, err := parsePrincipal(e, domain)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if len(out) == 0 && strings.TrimSpace(list) != "" {
		// Blank entries only (",") would otherwise leave the lists empty, which
		// means "anyone the provider admits": fail closed instead.
		return nil, fmt.Errorf("%q holds no principal; use one of %s", list, principalForms)
	}
	return out, nil
}

func parsePrincipal(e, domain string) (string, error) {
	if !isASCII(e) {
		return "", fmt.Errorf("%q is not ASCII; use one of %s", e, principalForms)
	}
	if strings.IndexFunc(e, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return "", fmt.Errorf("%q holds whitespace or a control character; use one of %s", e, principalForms)
	}
	e = strings.ToLower(e)
	provider, id, _ := strings.Cut(e, ":")
	switch provider {
	case "google":
		local, host, ok := strings.Cut(id, "@")
		if !ok || local == "" {
			return "", fmt.Errorf("%q is not an address; use one of %s", e, principalForms)
		}
		if domain == "" {
			return "", fmt.Errorf("google principals need --google-domain")
		}
		if host != domain {
			return "", fmt.Errorf("%q is outside the Workspace domain %s", e, domain)
		}
	case "github":
		if id == "" || strings.Trim(id, "0123456789") != "" {
			return "", fmt.Errorf("%q is not a numeric GitHub user id; use one of %s", e, principalForms)
		}
	case "azure":
		if !azureOID.MatchString(id) {
			return "", fmt.Errorf("%q is not an Entra object id (a GUID); use one of %s", e, principalForms)
		}
	default:
		return "", fmt.Errorf("%q has no known provider prefix; use one of %s", e, principalForms)
	}
	return e, nil
}

// RequireProviders fails when a principal names a provider that is not
// configured, so a grant can never sit unused behind a missing sign-in.
func (g AccessGrants) RequireProviders(google, github, azure bool) error {
	configured := map[string]bool{"google": google, "github": github, "azure": azure}
	for _, p := range slices.Concat(g.users, g.admins) {
		provider, _, _ := strings.Cut(p, ":")
		if !configured[provider] {
			return fmt.Errorf("access principal %q needs %s sign-in; set --%s-client-id", p, provider, provider)
		}
	}
	return nil
}

// Counts reports the list sizes, for the start-up log.
func (g AccessGrants) Counts() (users, admins int) { return len(g.users), len(g.admins) }

// principalOf is the normalised principal a session is matched as: the full
// address for Google (the session id is the account's sub), the session id for
// GitHub and Entra.
func principalOf(userID, username string) string {
	if isGoogleSession(userID) {
		return "google:" + strings.ToLower(username)
	}
	return strings.ToLower(userID)
}

func isGoogleSession(userID string) bool { return strings.HasPrefix(userID, "google:") }

// allowsUser reports whether a session may use the server at all, for every
// provider. With no lists the provider's own gate decides.
func (g AccessGrants) allowsUser(userID, username string) bool {
	if len(g.users) == 0 && len(g.admins) == 0 {
		return true
	}
	p := principalOf(userID, username)
	return slices.Contains(g.users, p) || slices.Contains(g.admins, p)
}

// isAdmin reports whether a session gets the admin interface by principal.
func (g AccessGrants) isAdmin(userID, username string) bool {
	return slices.Contains(g.admins, principalOf(userID, username))
}

// voiceWriters is the voice write allowlist for one session: the configuration
// cell's hashes, plus this user's hash when a grant makes them an admin.
func voiceWriters(writeAllowed []string, userID, username, userHash string) []string {
	if userHash == "" || !grants.isAdmin(userID, username) {
		return writeAllowed
	}
	return append(slices.Clone(writeAllowed), userHash)
}

// isASCII guards principal comparison: strings.ToLower folds some non-ASCII runes
// (the Kelvin sign) into ASCII letters, and Google issues ASCII addresses.
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// promptName is the name a model is told: a Google session is named by its full
// address for the grants, but only the local part goes into a prompt.
func promptName(userID, username string) string {
	if isGoogleSession(userID) {
		local, _, _ := strings.Cut(username, "@")
		return local
	}
	return username
}
