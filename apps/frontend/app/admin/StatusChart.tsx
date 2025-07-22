'use client';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';

const data = [
  { name: 'Enriquecidos', value: 18342 },
  { name: 'Parcialmente', value: 5120 },
  { name: 'RAW', value: 3124 },
  { name: 'Falhas', value: 2102 },
];

const COLORS = ['#10B981', '#F59E0B', '#3B82F6', '#EF4444'];

export default function StatusChart() {
  return (
    <div className="h-64 mt-4">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={60}
            outerRadius={80}
            paddingAngle={5}
            dataKey="value"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip 
            formatter={(value) => [value, 'Leads']}
            contentStyle={{
              backgroundColor: '#1F2937',
              borderColor: '#374151',
              borderRadius: '0.5rem',
            }}
          />
          <Legend 
            layout="vertical"
            verticalAlign="middle"
            align="right"
            wrapperStyle={{
              paddingLeft: '20px'
            }}
          />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}