import { mount } from 'svelte';
import App from './App.svelte';
import './app.css';

// Apply the saved theme before first paint (Sidebar keeps it in sync).
document.documentElement.dataset.theme = localStorage.getItem('burrow.theme') ?? 'dark';

const app = mount(App, {
  target: document.getElementById('app')!,
});

export default app;
