import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import Loader from './Loader.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Loader />
  </StrictMode>,
)
