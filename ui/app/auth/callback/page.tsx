"use client"

import { useEffect, useState } from "react"
import { GhostGraph } from "@/components/ghost-graph"
import { SESSION_TOKEN_KEY, consumeLogin } from "@/lib/api"
import { asset, sameOriginPath } from "@/lib/utils"

/**
 * Landing point for a cross-origin login hand-off.
 *
 * The OAuth callback itself never comes here — it goes to X-Ray's own
 * `--public-url + /v1/auth/github/callback`, which stays the single URL
 * registered with the OAuth app no matter how many UIs exist. X-Ray mints the
 * session and, when the login carried an allowlisted `?ui=<origin>`, bounces
 * the browser here with the token.
 *
 * That second hop is what lets a UI on another host share one API and one
 * OAuth app: the browser, not the cookie, carries the credential onward. A UI
 * served same-origin with the API — the deployed setup, where the UI server
 * proxies /v1 — never reaches this page. It already has the xray_session
 * cookie, which is the better credential precisely because JavaScript cannot
 * read it.
 *
 * A token is accepted ONLY against a login this tab started (see
 * `consumeLogin`). Accepting any token in the URL would mean a single link
 * could sign a visitor into the attacker's account and quietly harvest
 * everything they wrote next.
 */

/** Messages for the error codes the API can send. */
const ERROR_MESSAGES: Record<string, string> = {
  access_denied: "The sign-in was cancelled.",
  invalid_origin: "This UI's origin is not allowlisted on the API.",
  invalid_state: "The sign-in could not be matched to this browser.",
  server_error: "The identity provider reported an error.",
}

/**
 * Never render the raw `error` value. It is attacker-controlled text on a
 * page carrying the X-Ray logo, which makes it ready-made phishing furniture
 * ("Your account is suspended, call …"). Unknown codes collapse to one
 * generic line.
 */
function errorMessage(code: string): string {
  return ERROR_MESSAGES[code] ?? "The sign-in did not complete."
}

export default function AuthCallbackPage() {
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    // Read the fragment as well as the query. A fragment is never sent to a
    // server, so it never lands in an ingress or proxy access log; this
    // accepts that form already, so the API can move to it without a
    // matching UI release.
    const query = new URLSearchParams(window.location.search)
    const fragment = new URLSearchParams(window.location.hash.replace(/^#/, ""))
    const param = (name: string) => fragment.get(name) ?? query.get(name)

    // The disables below are unavoidable: window is only readable after mount
    // (this page is prerendered by the static export), so the token cannot be
    // parsed in a render-time initializer. The happy path never sets state at
    // all — it navigates away.
    const err = param("error")
    if (err) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setError(errorMessage(err))
      return
    }

    const token = param("token")
    if (!token) {
      setError("No token in the callback — the sign-in did not complete.")
      return
    }

    // Bind the token to a login THIS tab started. Single-use, and expired
    // markers are refused.
    if (!consumeLogin(param("state"))) {
      setError(
        "This sign-in did not start here, so the token was discarded. " +
        "Open the sign-in page and try again.",
      )
      return
    }

    try {
      sessionStorage.setItem(SESSION_TOKEN_KEY, token)
    } catch {
      setError("Could not store the session — browser storage is blocked.")
      return
    }

    // replace(), not assign(): this URL carries the token in its query string,
    // and this keeps it out of history where it would outlive the tab and be
    // recoverable with the Back button.
    window.location.replace(sameOriginPath(param("next"), window.location.origin))
  }, [])

  return (
    <div className="relative flex h-dvh items-center justify-center bg-black overflow-hidden">
      <GhostGraph />
      <div className="relative z-10 flex flex-col items-center gap-4 px-6 text-center">
        <img src={asset("/logo.svg")} alt="X-Ray" className="h-12 w-12 opacity-80" />
        {error ? (
          <>
            <h1 className="text-xl font-semibold tracking-tight text-white">Sign-in failed</h1>
            <p className="max-w-sm text-sm text-white/50">{error}</p>
            <a
              href={asset("/login")}
              className="rounded-lg border border-white/20 bg-white/5 px-6 py-2 text-sm font-medium text-white transition-colors hover:bg-white/10"
            >
              Try again
            </a>
          </>
        ) : (
          <p className="text-sm text-white/50">Completing sign-in…</p>
        )}
      </div>
    </div>
  )
}
