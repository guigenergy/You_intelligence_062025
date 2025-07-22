'use client';

import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

export default function DemandaMensalChart({ data }: { data: { mes: string; demanda_kw: number }[] }) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={data}>
        <XAxis dataKey="mes" />
        <YAxis />
        <Tooltip />
        <Line type="monotone" dataKey="demanda_kw" stroke="#f97316" strokeWidth={2} />
      </LineChart>
    </ResponsiveContainer>
  );
}
