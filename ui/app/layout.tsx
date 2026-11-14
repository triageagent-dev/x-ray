import type { Metadata, Viewport } from "next"
import { Inter } from "next/font/google"
import { Toaster } from "@/components/ui/sonner"
import { CategoryConfigLoader } from "@/components/category-config-loader"
import { TenantProvider } from "@/components/tenant-provider"
import { API_MOUNT, asset } from "@/lib/utils"
import "./globals.css"

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" })

export const metadata: Metadata = {
  title: "X-Ray",
  description: "X-Ray memory runtime",
  // asset(): metadata icon URLs are literals Next does not prefix with
  // basePath, so under a sub-path they would be asked of the API.
  icons: {
    icon: asset("/logo.svg"),
    apple: { url: asset("/apple-touch-icon.png"), sizes: "180x180", type: "image/png" },
  },
  manifest: `${API_MOUNT}/manifest.json`,
  appleWebApp: { capable: true, statusBarStyle: "black-translucent", title: "X-Ray" },
  other: { "mobile-web-app-capable": "yes" },
}

// viewport-fit=cover lets the mobile layout use env(safe-area-inset-*) for
// notch / home-indicator padding when installed/standalone.
export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
  themeColor: "#000000",
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <script dangerouslySetInnerHTML={{
          __html: `if('serviceWorker' in navigator){window.addEventListener('load',()=>{navigator.serviceWorker.register('${API_MOUNT}/sw.js').catch(()=>{})})}`,
        }} />
      </head>
      <body className={`${inter.variable} font-sans`}>
        <CategoryConfigLoader />
        <TenantProvider>{children}</TenantProvider>
        <Toaster />
      </body>
    </html>
  )
}
