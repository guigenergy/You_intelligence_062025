'use client'

import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { MapPin } from 'lucide-react'
import { Lead } from '@/app/types/lead'
import { useRouter } from 'next/navigation'
import { MapRef } from 'react-map-gl/maplibre'
import { useEffect, useRef, useState } from 'react'
import { clsx } from 'clsx'
import Map, { Marker, Popup, NavigationControl } from 'react-map-gl/maplibre'
import { geocodificarCEP } from '@/utils/geocode'

type Props = {
  leads: Lead[]
  selectedId?: string | null
}

const MAP_STYLE = 'https://tiles.stadiamaps.com/styles/alidade_smooth_dark.json'

type LeadComCoords = Lead & { lat: number; lng: number }

export default function MapView({ leads, selectedId }: Props) {
  const [pontos, setPontos] = useState<LeadComCoords[]>([])
  const [selecionado, setSelecionado] = useState<LeadComCoords | null>(null)
  const mapRef = useRef<MapRef>(null)
  const router = useRouter()

  useEffect(() => {
    async function carregarPontos() {
      const leadsValidos = leads.filter((l) => !!l.cep)


      const pontos: LeadComCoords[] = leadsValidos
      .filter((l) => l.latitude !== undefined && l.longitude !== undefined)
      .map((l) => ({ ...l, lat: Number(l.latitude), lng: Number(l.longitude) }))

      setPontos(pontos)
    }

    carregarPontos()
  }, [leads])

  return (
    <Map
      ref={mapRef}
      mapLib={maplibregl}
      mapStyle={MAP_STYLE}
      initialViewState={{ longitude: -51.23, latitude: -30.02, zoom: 5 }}
      style={{ width: '100%', height: '100%' }}
    >
      <NavigationControl position="top-left" />

      {pontos.map((l) => (
        <Marker
          key={l.id}
          longitude={l.lng}
          latitude={l.lat}
          anchor="bottom"
          draggable={false}
        >
          <div
            onMouseEnter={() => setSelecionado(l)}
            onMouseLeave={() => setSelecionado(null)}
            onClick={() => router.push(`/leads?id=${l.id}`)}
            className="cursor-pointer"
          >
            <MapPin
              className={clsx(
                'w-6 h-6 drop-shadow-lg transition-all',
                l.id === selectedId ? 'text-lime-400 scale-125' : 'text-red-600'
              )}
            />
          </div>
        </Marker>
      ))}

      {selecionado && (
        <Popup
          longitude={selecionado.lng}
          latitude={selecionado.lat}
          anchor="top"
          closeButton={false}
          offset={[0, 10]}
        >
          <p className="text-xs text-black-500">DIC: {selecionado.dicMed}</p>
          <p className="text-xs text-black-500">FIC: {selecionado.ficMed}</p>
          <p className="text-xs text-black-500">CNAE: {selecionado.cnae}</p>
          <p className="text-xs text-black-500">Estado: {selecionado.estado}</p>
          <p className="text-xs text-black-500">Distribuidora: {selecionado.distribuidora}</p>
        </Popup>
      )}
    </Map>
  )
}
