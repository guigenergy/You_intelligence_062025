// components/layout/Sidebar.tsx
'use client';

import Link from 'next/link';
import Image from 'next/image';
import { usePathname } from 'next/navigation';
import { clsx } from 'clsx';
import {
  Home,
  MapPinned,
  BarChart2,
  MapIcon,
  LayoutDashboard,
} from 'lucide-react';

const links = [
  { href: '/',        label: 'Dashboard',  icon: LayoutDashboard },
  //{ href: '/dashboard',label: 'Dashboard',       icon: LayoutDashboard },
  { href: '/mapa',    label: 'Mapa interativo',  icon: MapPinned },
  { href: '/leads',   label: 'Dados',            icon: BarChart2 },
   { href: '/mapaHeat', label: 'Mapa de calor',  icon: MapIcon },

  //{ href: '/noticias',label: 'Notícias',         icon: Newspaper },
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed inset-y-0 left-0 w-60 bg-black text-white flex flex-col border-r border-white/10">
      {/* topo logo ------------------------------------------------------ */}
      <div className="h-16 flex items-center px-6">
        <Image
          src="/logo-verde.png"      
          alt="You.On"
          height={28}
          width={120}
          priority
        />
      </div>

      {/* navegação ------------------------------------------------------ */}
      <nav className="flex-1 mt-4 space-y-1 px-2">
        {links.map(({ href, label, icon: Icon }) => {
          const active = pathname === href;
          return (
            <Link
              key={href}
              href={href}
              className={clsx(
                'flex items-center gap-3 px-3 py-2 rounded-md transition',
                active
                  ? 'bg-white/10 font-medium'
                  : 'hover:bg-white/5 text-gray-300 hover:text-white'
              )}
            >
              <Icon
                size={18}
                className={clsx(
                  'shrink-0',
                  active ? 'text-[#b3d430]' : 'text-[#b3d430]/70'
                )}
              />
              <span className="truncate">{label}</span>
            </Link>
          );
        })}
      </nav>
      <div className="border-t border-white/10 p-4">
        {true && ( // Substitua por user?.role === 'admin'
          <Link
            href="/admin"
            className="flex items-center gap-3 px-3 py-2 rounded-md transition hover:bg-white/5 text-gray-300 hover:text-white"
          >
            <Image
              src="/icone-verde.png"
              alt="Admin"
              width={28}
              height={28}
              className="rounded-full border border-zinc-700"
            />
            <span className="truncate">Admin</span>
          </Link>
        )}
      </div>
    </aside>
  );
}
