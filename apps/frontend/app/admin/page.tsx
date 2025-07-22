'use client';
import { useState } from 'react';
import dynamic from 'next/dynamic';
import SelectDistribuidoras from './SelectDistribuidoras';
import SelectAnos from './SelectAnos';
import ButtonImportar from './ButtonImportar';
import TabelaStatusImportacoes from './TabelaStatusImportacoes';
import PainelEnriquecimento from './PainelEnriquecimento';

// Carregamento dinâmico para charts (melhor performance)
const LeadsChart = dynamic(() => import('./LeadsChart'), { 
  ssr: false,
  loading: () => <div className="h-64 bg-zinc-900/50 rounded-xl animate-pulse"></div>
});

const StatusChart = dynamic(() => import('./StatusChart'), {
  ssr: false,
  loading: () => <div className="h-64 bg-zinc-900/50 rounded-xl animate-pulse"></div>
});

export default function AdminPage() {
  const [distribuidorasSelecionadas, setDistribuidorasSelecionadas] = useState<string[]>([]);
  const [anosSelecionados, setAnosSelecionados] = useState<number[]>([]);
  const [activeTab, setActiveTab] = useState<'importacao' | 'leads'>('importacao');

  return (
    <div className="min-h-screen bg-zinc-950 p-4 md:p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <header className="text-center">
          <h1 className="text-2xl md:text-3xl font-bold text-white mb-2 bg-gradient-to-r from-blue-500 to-emerald-500 bg-clip-text text-transparent">
            Painel de Administração de Dados
          </h1>
          <p className="text-zinc-400 text-sm md:text-base">
            Monitoramento completo de importações e base de leads
          </p>
        </header>

        {/* Tabs */}
        <div className="flex border-b border-zinc-800">
          <button
            onClick={() => setActiveTab('importacao')}
            className={`px-4 py-2 font-medium text-sm ${activeTab === 'importacao' ? 'text-blue-400 border-b-2 border-blue-500' : 'text-zinc-400 hover:text-white'}`}
          >
            Controle de Importação
          </button>
          <button
            onClick={() => setActiveTab('leads')}
            className={`px-4 py-2 font-medium text-sm ${activeTab === 'leads' ? 'text-blue-400 border-b-2 border-blue-500' : 'text-zinc-400 hover:text-white'}`}
          >
            Análise de Leads
          </button>
        </div>

        {activeTab === 'importacao' ? (
          <>
            {/* Import Section */}
            <section className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4 md:p-6 shadow-lg">
              <div className="flex flex-col md:flex-row gap-4 md:gap-6 items-start md:items-end">
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 w-full">
                  <div className="space-y-1">
                    <h3 className="text-xs md:text-sm font-medium text-zinc-400">Distribuidoras</h3>
                    <SelectDistribuidoras onChange={setDistribuidorasSelecionadas} />
                  </div>
                  <div className="space-y-1">
                    <h3 className="text-xs md:text-sm font-medium text-zinc-400">Anos</h3>
                    <SelectAnos onChange={setAnosSelecionados} />
                  </div>
                  <div className="flex items-end h-full">
                    <ButtonImportar 
                      distribuidoras={distribuidorasSelecionadas} 
                      anos={anosSelecionados} 
                    />
                  </div>
                </div>
              </div>
            </section>

            {/* Status Section */}
            <section className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4 md:p-6 shadow-lg">
              <h2 className="text-lg md:text-xl font-semibold text-white mb-4">
                Status das Importações
              </h2>
              <TabelaStatusImportacoes />
            </section>

            {/* Enrichment Section */}
            <section className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4 md:p-6 shadow-lg">
              <h2 className="text-lg md:text-xl font-semibold text-white mb-4">
                Enriquecimento de Dados
              </h2>
              <PainelEnriquecimento />
            </section>
          </>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Gráfico de Leads por Status */}
            <section className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4 md:p-6 shadow-lg">
              <div className="flex justify-between items-center mb-4">
                <h2 className="text-lg md:text-xl font-semibold text-white">
                  Leads por Status
                </h2>
                <span className="text-xs text-zinc-400">Atualizado em tempo real</span>
              </div>
              <StatusChart />
            </section>

            {/* Gráfico de Leads por Distribuidora */}
            <section className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4 md:p-6 shadow-lg">
              <h2 className="text-lg md:text-xl font-semibold text-white mb-4">
                Leads por Distribuidora
              </h2>
              <LeadsChart />
            </section>

            {/* Métricas Rápidas */}
            <section className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4 md:p-6 shadow-lg lg:col-span-2">
              <h2 className="text-lg md:text-xl font-semibold text-white mb-4">
                Métricas de Leads
              </h2>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <MetricCard 
                  title="Total de Leads" 
                  value="24.568" 
                  change="+12%"
                  icon={<div className="bg-blue-500/20 p-2 rounded-lg">📊</div>}
                />
                <MetricCard 
                  title="Enriquecidos" 
                  value="18.342" 
                  change="+8%"
                  icon={<div className="bg-green-500/20 p-2 rounded-lg">✔️</div>}
                />
                <MetricCard 
                  title="Pendentes" 
                  value="3.124" 
                  change="-4%"
                  icon={<div className="bg-yellow-500/20 p-2 rounded-lg">⏳</div>}
                />
                <MetricCard 
                  title="Com Falhas" 
                  value="2.102" 
                  change="+2%"
                  icon={<div className="bg-red-500/20 p-2 rounded-lg">⚠️</div>}
                />
              </div>
            </section>
          </div>
        )}
      </div>
    </div>
  );
}

// Componente para cards de métricas
function MetricCard({ title, value, change, icon }: { title: string; value: string; change: string; icon: React.ReactNode }) {
  const isPositive = change.startsWith('+');
  
  return (
    <div className="bg-zinc-800/50 p-4 rounded-lg border border-zinc-800">
      <div className="flex justify-between items-start">
        <div>
          <p className="text-xs text-zinc-400">{title}</p>
          <p className="text-xl font-bold text-white mt-1">{value}</p>
        </div>
        {icon}
      </div>
      <p className={`text-xs mt-3 ${isPositive ? 'text-green-400' : 'text-red-400'}`}>
        {change} vs último mês
      </p>
    </div>
  );
}