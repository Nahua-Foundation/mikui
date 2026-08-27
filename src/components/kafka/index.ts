// Types
export * from './types';

// Стартовые данные (без персистентности — см. Фазу 3)
export * from './mockData';

// Hooks
export { useMessageWindow } from './useMessageWindow';

// Components
export { DialogContentNoClose } from './DialogContentNoClose';
export { HeaderDesktop } from './Header';
export { TopicsPanel } from './TopicsPanel';
export { MessagesPanel } from './MessagesPanel';

// Modals
export { MessageDetailsModal } from './modals/MessageDetailsModal';
export { TopicConfigModal } from './modals/TopicConfigModal';
export { ClusterConfigModal } from './modals/ClusterConfigModal';
export { FavoritesModal } from './modals/FavoritesModal';

// Small Components
export { Tab } from './components/Tab';
export { MenuItem } from './components/MenuItem';
