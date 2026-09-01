import { Button } from '../../ui/button';
import { Plus, Settings, Trash2 } from 'lucide-react';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { KafkaCluster } from '../types';
import * as api from '../api';

interface ClusterArchiveModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  clusters: KafkaCluster[];
  connectedClusterId: string | null;
  onDeleteCluster: (id: string) => void;
  onCreateNew: () => void;
  onEditCluster: (cluster: KafkaCluster) => void;
  onConnectToCluster: (cluster: KafkaCluster) => void;
}

export function ClusterArchiveModal({
  open,
  onOpenChange,
  clusters,
  connectedClusterId,
  onDeleteCluster,
  onCreateNew,
  onEditCluster,
  onConnectToCluster,
}: ClusterArchiveModalProps) {
  const handleConnectAndClose = (cluster: KafkaCluster) => {
    onConnectToCluster(cluster);
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-4xl max-h-[80vh] bg-surface border-edge text-slate-50">
        <DialogHeader className="border-b border-edge pb-4">
          <div className="flex items-center justify-between">
            <div>
              <DialogTitle className="font-mono text-soft text-xl">
                Cluster Archive
              </DialogTitle>
              <DialogDescription className="text-sm text-dim">
                Manage your Kafka cluster connections
              </DialogDescription>
            </div>
            <Button
              onClick={onCreateNew}
              className="bg-brand text-surface hover:bg-brand-hover font-mono"
            >
              <Plus className="size-4" />
              Create New
            </Button>
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-auto pt-4">
          {/* Не таблица: строка адресов брокеров бывает длиннее модалки, а
              `<table>` в таком случае растягивается по самой длинной ячейке и
              уезжает под горизонтальный скролл — вместе с кнопками, которые
              обязаны оставаться на виду. Flex с `min-w-0` держит их справа, а
              длинному тексту даёт усечься; полное значение — в подсказке. */}
          {/* Пустая рамка вместо списка выглядела бы как сломанная вёрстка. */}
          <div
            className={`bg-surface border border-edge rounded-lg overflow-hidden ${
              clusters.length === 0 ? 'hidden' : ''
            }`}
          >
            {clusters.map((cluster) => {
              // Под кем откроется этот кластер по клику — то же, что решит
              // и сам обработчик подключения.
              const user = api.activeUser(cluster);
              const details = [
                cluster.brokers,
                cluster.security_protocol,
                user && `${user.username}${cluster.users.length > 1 ? ` +${cluster.users.length - 1}` : ''}`,
              ]
                .filter(Boolean)
                .join(' · ');

              return (
                <div
                  key={cluster.id}
                  className="flex items-center gap-3 px-4 py-4 border-b border-edge last:border-b-0 hover:bg-edge/20 cursor-pointer transition-colors"
                  onClick={() => handleConnectAndClose(cluster)}
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      {cluster.id === connectedClusterId && (
                        <span
                          className="size-2 rounded-full bg-green-500 shrink-0"
                          title="Connected"
                        />
                      )}
                      <span className="font-mono text-slate-50 truncate" title={cluster.name}>
                        {cluster.name}
                      </span>
                    </div>
                    <div className="font-mono text-xs text-dim mt-1 truncate" title={details}>
                      {details}
                    </div>
                  </div>

                  <div className="flex items-center gap-2 shrink-0">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        onEditCluster(cluster);
                      }}
                      className="p-1 text-dim hover:text-brand transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
                      title="Connection settings"
                    >
                      <Settings className="size-4" />
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        onDeleteCluster(cluster.id);
                      }}
                      className="p-1 text-dim hover:text-red-400 transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
                      title="Delete cluster"
                    >
                      <Trash2 className="size-4" />
                    </button>
                  </div>
                </div>
              );
            })}
          </div>

          {clusters.length === 0 && (
            <div className="text-center py-12">
              <div className="text-dim font-mono mb-4">
                No clusters configured yet
              </div>
              <Button
                onClick={onCreateNew}
                className="bg-brand text-surface hover:bg-brand-hover font-mono"
              >
                <Plus className="size-4 mr-2" />
                Create Your First Cluster
              </Button>
            </div>
          )}
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}