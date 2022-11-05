/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: false,
  swcMinify: false,
  experimental: {
    esmExternals: "loose",
  },
};

module.exports = nextConfig;
