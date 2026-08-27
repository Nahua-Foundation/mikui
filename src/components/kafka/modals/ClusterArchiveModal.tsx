import { Button } from '../../ui/button';
import { Plus, Edit, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { KafkaCluster } from '../types';
import { Table, TableBody, TableCell, TableRow } from '../../ui/table';

interface ClusterArchiveModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  clusters: KafkaCluster[];
  onClustersChange: (clusters: KafkaCluster[]) => void;
  onCreateNew: () => void;
  onEditCluster: (cluster: KafkaCluster) => void;
  onConnectToCluster: (cluster: KafkaCluster) => void;
}

export function ClusterArchiveModal({ 
  open, 
  onOpenChange, 
  clusters, 
  onClustersChange, 
  onCreateNew, 
  onEditCluster, 
  onConnectToCluster 
}: ClusterArchiveModalProps) {
  const handleDeleteCluster = (clusterId: string) => {
    const updatedClusters = clusters.filter(cluster => cluster.id !== clusterId);
    onClustersChange(updatedClusters);
    toast.success('Cluster deleted successfully');
  };

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
              <Plus className="size-4 mr-2" />
              Create New
            </Button>
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-auto pt-4">
          <div className="bg-surface border border-edge rounded-lg overflow-hidden">
            <Table>
              <TableBody>
                {clusters.map((cluster) => (
                  <TableRow
                    key={cluster.id}
                    className="border-edge hover:bg-edge/20 cursor-pointer transition-colors"
                    onClick={() => handleConnectAndClose(cluster)}
                  >
                    <TableCell className="py-4">
                      <div className="font-mono text-slate-50">
                        {cluster.name}
                      </div>
                    </TableCell>
                    <TableCell className="py-4 text-right">
                      <div className="flex items-center justify-end gap-2">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            onEditCluster(cluster);
                          }}
                          className="p-1 text-dim hover:text-brand transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
                          title="Edit cluster"
                        >
                          <Edit className="size-4" />
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDeleteCluster(cluster.id);
                          }}
                          className="p-1 text-dim hover:text-red-400 transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
                          title="Delete cluster"
                        >
                          <Trash2 className="size-4" />
                        </button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
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