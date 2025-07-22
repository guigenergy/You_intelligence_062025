'use client';

import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

export default function EnergiaMensalChart({ data }: { data: { mes: string; energia_kwh: number }[] }) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={data}>
        <XAxis dataKey="mes" />
        <YAxis />
        <Tooltip />
        <Line type="monotone" dataKey="energia_kwh" stroke="#60a5fa" strokeWidth={2} />
      </LineChart>
    </ResponsiveContainer>
  );
}
