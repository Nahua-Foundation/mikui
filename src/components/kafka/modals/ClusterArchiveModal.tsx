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
      <DialogContentNoClose className="max-w-4xl max-h-[80vh] bg-[#0f172b] border-[#314158] text-slate-50">
        <DialogHeader className="border-b border-[#314158] pb-4">
          <div className="flex items-center justify-between">
            <div>
              <DialogTitle className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-xl">
                Cluster Archive
              </DialogTitle>
              <DialogDescription className="text-sm text-[#62748E]">
                Manage your Kafka cluster connections
              </DialogDescription>
            </div>
            <Button
              onClick={onCreateNew}
              className="bg-[#ffb86a] text-[#0f172b] hover:bg-[#e5a860] font-['Fira_Code:Retina',_sans-serif]"
            >
              <Plus className="size-4 mr-2" />
              Create New
            </Button>
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-auto pt-4">
          <div className="bg-[#0f172b] border border-[#314158] rounded-lg overflow-hidden">
            <Table>
              <TableBody>
                {clusters.map((cluster) => (
                  <TableRow
                    key={cluster.id}
                    className="border-[#314158] hover:bg-[#314158]/20 cursor-pointer transition-colors"
                    onClick={() => handleConnectAndClose(cluster)}
                  >
                    <TableCell className="py-4">
                      <div className="font-['Fira_Code:Retina',_sans-serif] text-slate-50">
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
                          className="p-1 text-[#62748E] hover:text-[#ffb86a] transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
                          title="Edit cluster"
                        >
                          <Edit className="size-4" />
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDeleteCluster(cluster.id);
                          }}
                          className="p-1 text-[#62748E] hover:text-red-400 transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
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
              <div className="text-[#62748E] font-['Fira_Code:Retina',_sans-serif] mb-4">
                No clusters configured yet
              </div>
              <Button
                onClick={onCreateNew}
                className="bg-[#ffb86a] text-[#0f172b] hover:bg-[#e5a860] font-['Fira_Code:Retina',_sans-serif]"
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