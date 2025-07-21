// tailwind.config.js
/** @type {import('tailwindcss').Config} */
module.exports = {
 content: [
  "./app/**/*.{js,ts,jsx,tsx}",
  "./pages/**/*.{js,ts,jsx,tsx}",
  "./components/**/*.{js,ts,jsx,tsx}",
  "./apps/frontend/app/**/*.{js,ts,jsx,tsx}",
  "./apps/frontend/pages/**/*.{js,ts,jsx,tsx}",
  "./apps/frontend/components/**/*.{js,ts,jsx,tsx}",
],
  theme: {
    extend: {
      colors: {
        primary: '#00ff00',
        secondary: '#00cc00',
      },
    },
  },
  plugins: [],
};