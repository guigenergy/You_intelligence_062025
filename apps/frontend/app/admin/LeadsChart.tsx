'use client';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';

const data = [
  { name: 'ENEL', value: 8452 },
  { name: 'CPFL', value: 7210 },
  { name: 'LIGHT', value: 5231 },
  { name: 'ELETROPAULO', value: 3675 },
];

export default function LeadsChart() {
  return (
    <div className="h-64 mt-4">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis 
            dataKey="name" 
            stroke="#9CA3AF"
            tick={{ fontSize: 12 }}
          />
          <YAxis 
            stroke="#9CA3AF"
            tick={{ fontSize: 12 }}
          />
          <Tooltip 
            contentStyle={{
              backgroundColor: '#1F2937',
              borderColor: '#374151',
              borderRadius: '0.5rem',
            }}
          />
          <Bar 
            dataKey="value" 
            fill="#3B82F6" 
            radius={[4, 4, 0, 0]}
            name="Leads"
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}