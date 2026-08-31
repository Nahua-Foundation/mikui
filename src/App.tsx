import { KafkaExplorerPortfolio } from "./components/KafkaExplorerPortfolio";
import { Toaster } from "./components/ui/sonner";

export default function App() {
  // Размеры и тема — в `styles/globals.css` и в `index.html`. Здесь их раньше
  // навешивали присваиванием в теле рендера: и класс темы, и высоты `html`/
  // `body`. Это выполнялось на каждую отрисовку, до первой не выполнялось
  // вовсе (отсюда мигание светлой темой) и никак не отменялось.
  return (
    <div className="h-full w-full overflow-hidden">
      <KafkaExplorerPortfolio />
      <Toaster />
    </div>
  );
}
