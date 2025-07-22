'use client';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts';
import { DISTRIBUIDORAS_MAP } from '@/utils/distribuidoras';

export default function BarLeadsDistribuidora({ data }: { data: { nome: string; total: number }[] }) {
  return (
    <div className="bg-zinc-900/50 border border-zinc-700 rounded-xl p-4 h-[350px]">
      <h3 className="text-lg font-semibold text-white mb-2">Leads por Distribuidora</h3>
      <p className="text-sm text-zinc-400 mb-4">Distribuição de leads por concessionária</p>
      
      <ResponsiveContainer width="100%" height="80%">
        <BarChart
          data={data}
          margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
          barSize={28}
        >
          <CartesianGrid 
            strokeDasharray="3 3" 
            stroke="#374151" 
            vertical={false} 
          />
          <XAxis 
            dataKey="nome" 
            tick={{ fill: '#9CA3AF', fontSize: 12 }}
            tickLine={false}
            axisLine={{ stroke: '#374151' }}
          />
          <YAxis 
            tick={{ fill: '#9CA3AF', fontSize: 12 }}
            tickLine={false}
            axisLine={{ stroke: '#374151' }}
          />
          <Tooltip 
            contentStyle={{
              background: 'rgba(17, 24, 39, 0.9)',
              borderColor: '#374151',
              borderRadius: '0.5rem',
              backdropFilter: 'blur(4px)',
            }}
            formatter={(value: number) => [
              <span key="value" className="text-emerald-400 font-bold">{value}</span>, 
              <span key="label">Leads</span>
            ]}
            labelFormatter={(label: string) => (
              <div className="text-white font-medium">
                {label} - {DISTRIBUIDORAS_MAP[label] ?? 'Segmento desconhecido'}
              </div>
            )}
          />
          <Bar 
            dataKey="total" 
            name="Total de Leads"
            radius={[4, 4, 0, 0]}
            background={{ fill: 'rgba(31, 41, 55, 0.5)', radius: 4 }}
          >
            {data.map((entry, index) => (
              <linearGradient
                key={`color-${index}`}
                id={`color-${index}`}
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop offset="0%" stopColor="#a3e635" stopOpacity={0.8}/>
                <stop offset="100%" stopColor="#65a30d" stopOpacity={0.8}/>
              </linearGradient>
            ))}
          </Bar>
          <Legend 
            formatter={(value) => (
              <span className="text-zinc-300 text-sm">{value}</span>
            )}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}