'use client';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell } from 'recharts';
import { CNAE_SEGMENTOS } from '@/utils/cnae';

const COLORS = ['#facc15', '#fbbf24', '#f59e0b', '#d97706', '#b45309'];

export default function BarTopCNAE({ data }: { data: { nome: string; total: number }[] }) {
  const dataComDescricao = data.map((item) => ({
    ...item,
    codigo: item.nome,
    descricao: CNAE_SEGMENTOS[item.nome] ?? 'Segmento desconhecido',
  }));

  return (
    <div className="bg-zinc-900/50 border border-zinc-700 rounded-xl p-4 h-[450px]">
      <h3 className="text-lg font-semibold text-white mb-1">Top Segmentos CNAE</h3>
      <p className="text-sm text-zinc-400 mb-4">Distribuição de leads por classificação CNAE</p>
      
      <ResponsiveContainer width="100%" height="80%">
        <BarChart
          layout="vertical"
          data={dataComDescricao}
          margin={{ top: 5, right: 20, left: 120, bottom: 5 }}
          barSize={24}
        >
          <CartesianGrid 
            strokeDasharray="3 3" 
            stroke="#374151" 
            horizontal={true}
            vertical={false}
          />
          <XAxis 
            type="number"
            tick={{ fill: '#9CA3AF', fontSize: 12 }}
            tickLine={false}
            axisLine={{ stroke: '#374151' }}
          />
          <YAxis 
            dataKey="codigo"
            type="category"
            width={140}
            tick={{ fill: '#E5E7EB', fontSize: 12 }}
            tickLine={false}
            axisLine={{ stroke: '#374151' }}
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
              <span key="value" className="text-yellow-400 font-bold">{value}</span>, 
              <span key="label" className="text-zinc-300">Leads</span>
            ]}
            labelFormatter={(label: string) => (
              <div className="text-white font-medium">
                {CNAE_SEGMENTOS[label] || 'Segmento desconhecido'}
                <div className="text-xs text-zinc-400 mt-1">CNAE: {label}</div>
              </div>
            )}
            cursor={{ fill: 'rgba(255, 255, 255, 0.1)' }}
          />
          <Bar 
            dataKey="total" 
            name="Total de Leads"
            radius={[0, 4, 4, 0]}
          >
            {dataComDescricao.map((entry, index) => (
              <Cell 
                key={`cell-${index}`}
                fill={COLORS[index % COLORS.length]}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}