'use client';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts';

export default function DICFICMensalChart({ data }: { data: { mes: string; dic: number; fic: number }[] }) {
  return (
    <div className="bg-zinc-900/50 border border-zinc-700 rounded-xl p-4 h-[400px]">
      <h3 className="text-lg font-semibold text-white mb-1">Indicadores Mensais</h3>
      <p className="text-sm text-zinc-400 mb-4">Duração e Frequência de Interrupções</p>
      
      <ResponsiveContainer width="100%" height="80%">
        <BarChart
          data={data}
          margin={{ top: 20, right: 20, left: 0, bottom: 5 }}
          barGap={4}
          barCategoryGap={12}
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
          />
          <Tooltip
            contentStyle={{
              background: 'rgba(17, 24, 39, 0.95)',
              borderColor: '#374151',
              borderRadius: '0.5rem',
              backdropFilter: 'blur(4px)',
              boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
            }}
            formatter={(value: number, name: string) => [
              <span key="value" className={`font-bold ${
                name === 'DIC (min)' ? 'text-emerald-400' : 'text-red-400'
              }`}>
                {value} {name.includes('DIC') ? 'min' : 'vezes'}
              </span>,
              name
            ]}
            labelFormatter={(label) => (
              <div className="text-white font-medium">
                Mês: {label}
              </div>
            )}
            cursor={{ fill: 'rgba(255, 255, 255, 0.05)' }}
          />
          <Bar 
            dataKey="dic"
            name="DIC (min)"
            radius={[4, 4, 0, 0]}
            background={{ fill: 'rgba(31, 41, 55, 0.3)', radius: 4 }}
          >
            {data.map((entry, index) => (
              <linearGradient
                key={`dic-color-${index}`}
                id={`dic-color-${index}`}
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop offset="0%" stopColor="#34d399" stopOpacity={0.9}/>
                <stop offset="100%" stopColor="#10b981" stopOpacity={0.9}/>
              </linearGradient>
            ))}
          </Bar>
          <Bar 
            dataKey="fic"
            name="FIC (vezes)"
            radius={[4, 4, 0, 0]}
            background={{ fill: 'rgba(31, 41, 55, 0.3)', radius: 4 }}
          >
            {data.map((entry, index) => (
              <linearGradient
                key={`fic-color-${index}`}
                id={`fic-color-${index}`}
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop offset="0%" stopColor="#f87171" stopOpacity={0.9}/>
                <stop offset="100%" stopColor="#ef4444" stopOpacity={0.9}/>
              </linearGradient>
            ))}
          </Bar>
          <Legend 
            formatter={(value) => (
              <span className="text-zinc-300 text-sm">{value}</span>
            )}
            wrapperStyle={{ paddingTop: '20px' }}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}