'use client';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts';

export default function DemandaMensalChart({ data }: { data: { mes: string; demanda_kw: number }[] }) {
  return (
    <div className="bg-zinc-900/50 border border-zinc-700 rounded-xl p-4 h-[350px]">
      <h3 className="text-lg font-semibold text-white mb-1">Demanda Mensal (kW)</h3>
      <p className="text-sm text-zinc-400 mb-4">Variação da demanda energética por mês</p>
      
      <ResponsiveContainer width="100%" height="80%">
        <LineChart
          data={data}
          margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
        >
          <CartesianGrid 
            strokeDasharray="3 3" 
            stroke="#374151" 
            vertical={false}
          />
          <XAxis 
            dataKey="mes"
            tick={{ fill: '#9CA3AF', fontSize: 12 }}
            tickLine={false}
            axisLine={{ stroke: '#374151' }}
          />
          <YAxis 
            tick={{ fill: '#9CA3AF', fontSize: 12 }}
            tickLine={false}
            axisLine={{ stroke: '#374151' }}
            width={40}
          />
          <Tooltip
            contentStyle={{
              background: 'rgba(17, 24, 39, 0.95)',
              borderColor: '#374151',
              borderRadius: '0.5rem',
              backdropFilter: 'blur(4px)',
              boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
            }}
            formatter={(value: number) => [
              <span key="value" className="text-orange-400 font-bold">{value} kW</span>
            ]}
            labelFormatter={(label) => (
              <div className="text-white font-medium">
                Mês: {label}
              </div>
            )}
            cursor={{ stroke: '#f97316', strokeWidth: 1, strokeDasharray: '3 3' }}
          />
          <Line 
            type="monotone"
            dataKey="demanda_kw"
            name="Demanda (kW)"
            stroke="#f97316"
            strokeWidth={3}
            dot={{ r: 4, fill: '#f97316', strokeWidth: 2 }}
            activeDot={{ r: 6, fill: '#fff', stroke: '#f97316', strokeWidth: 2 }}
          />
          <Legend 
            formatter={(value) => (
              <span className="text-zinc-300 text-sm">{value}</span>
            )}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}