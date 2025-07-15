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
      maxWidth:{
        'container': '77.5rem'
      },
      colors:{
        'gray-100': '#F2F4F7',
        'gray-300': '#D0D5DD',
        'gray-600': '#475467',
        'gray-700': '#D0D5DD',
        'gray-900': '#101828',
        'brand-50': '#F9F5FF',
        'brand-600': '#7F56D9',
        'brand-700': '#6941C6',
        
      }
    },
  },
  plugins: [],
};