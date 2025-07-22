'use client';

import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer } from 'recharts';

export default function DICFICMensalChart({ data }: { data: { mes: string; dic: number; fic: number }[] }) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data}>
        <XAxis dataKey="mes" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Bar dataKey="dic" fill="#34d399" name="DIC (min)" />
        <Bar dataKey="fic" fill="#f87171" name="FIC (vezes)" />
      </BarChart>
    </ResponsiveContainer>
  );
}
