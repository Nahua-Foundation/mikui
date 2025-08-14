import { KafkaExplorerPortfolio } from "./components/KafkaExplorerPortfolio";
import { Toaster } from "./components/ui/sonner";

export default function App() {
  // Add dark class to body for dark theme
  if (typeof document !== 'undefined') {
    document.body.classList.add('dark');
    document.documentElement.style.height = '100%';
    document.body.style.height = '100%';
    document.body.style.margin = '0';
    document.body.style.padding = '0';
  }

  return (
    <div className="h-screen w-screen overflow-hidden">
      <KafkaExplorerPortfolio />
      <Toaster />
    </div>
  );
}