import React, {
  useEffect,
  useImperativeHandle,
  useMemo,
  useState,
  forwardRef,
} from "react";
import { useSettings } from "@/contexts/SettingsContext";
import type { Settings as SettingsData } from "@/types/coreTypes";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { InfoIcon } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { toast } from "@/components/ui/use-toast";

interface SettingsFormProps {
  isEditing: boolean;
  onChange?: () => void;
}

type GpuVariant = "None" | "A100 40G" | "A100 80G" | "H100 80G" | "H200 141G";

const CPU_OPTIONS = [
  { label: "2vCPU / 8G RAM", value: "n4-standard-2" },
  { label: "4vCPU / 16G RAM", value: "n4-standard-4" },
  { label: "8vCPU / 32G RAM", value: "n4-standard-8" },
  { label: "16vCPU / 64G RAM", value: "n4-standard-16" },
  { label: "32vCPU / 128G RAM", value: "n4-standard-32" },
  { label: "64vCPU / 256G RAM", value: "n4-standard-64" },
  { label: "80vCPU / 320G RAM", value: "n4-standard-80" },
] as const;

const GPU_CPU_MAP: Record<string, { label: string; value: string }> = {
  "1x A100 40G": { label: "12vCPU / 85G RAM", value: "a2-highgpu-1g" },
  "2x A100 40G": { label: "24vCPU / 170G RAM", value: "a2-highgpu-2g" },
  "4x A100 40G": { label: "48vCPU / 340G RAM", value: "a2-highgpu-4g" },
  "8x A100 40G": { label: "96vCPU / 680G RAM", value: "a2-highgpu-8g" },

  "1x A100 80G": { label: "12vCPU / 170G RAM", value: "a2-ultragpu-1g" },
  "2x A100 80G": { label: "24vCPU / 340G RAM", value: "a2-ultragpu-2g" },
  "4x A100 80G": { label: "48vCPU / 680G RAM", value: "a2-ultragpu-4g" },
  "8x A100 80G": { label: "96vCPU / 1360G RAM", value: "a2-ultragpu-8g" },

  "1x H100 80G": { label: "26vCPU / 234G RAM", value: "a3-highgpu-1g" },
  "2x H100 80G": { label: "52vCPU / 468G RAM", value: "a3-highgpu-2g" },
  "4x H100 80G": { label: "104vCPU / 936G RAM", value: "a3-highgpu-4g" },
  "8x H100 80G": { label: "208vCPU / 1872G RAM", value: "a3-highgpu-8g" },

  "8x H200 141G": { label: "224vCPU / 2952G RAM", value: "a3-ultragpu-8g" },
};

type RegionOption = { value: string; label: string };

const REGION_OPTIONS: Record<GpuVariant, RegionOption[]> = {
  "A100 40G": [
    { value: "us-central1", label: "us-central1" },
    { value: "us-west3", label: "us-west3" },
    { value: "us-east1", label: "us-east1" },
    { value: "us-west4", label: "us-west4" },
    { value: "us-west1", label: "us-west1" },
    { value: "europe-west4", label: "europe-west4" },
    { value: "asia-northeast1", label: "asia-northeast1" },
    { value: "asia-northeast3", label: "asia-northeast3" },
    { value: "me-west1", label: "me-west1" },
    { value: "asia-southeast1", label: "asia-southeast1" },
  ],
  "A100 80G": [
    { value: "us-central1", label: "us-central1" },
    { value: "us-east5", label: "us-east5" },
    { value: "us-east4", label: "us-east4" },
    { value: "europe-west4", label: "europe-west4" },
    { value: "asia-southeast1", label: "asia-southeast1" },
  ],
  "H100 80G": [
    { value: "us-central1", label: "us-central1" },
    { value: "us-east5", label: "us-east5" },
    { value: "us-east4", label: "us-east4" },
    { value: "us-west4", label: "us-west4" },
    { value: "us-west1", label: "us-west1" },
    { value: "europe-west1", label: "europe-west1" },
    { value: "asia-northeast1", label: "asia-northeast1" },
    { value: "asia-southeast1", label: "asia-southeast1" },
  ],
  "H200 141G": [
    { value: "us-central1", label: "us-central1" },
    { value: "us-south1", label: "us-south1" },
    { value: "us-east4", label: "us-east4" },
    { value: "us-east1", label: "us-east1" },
    { value: "us-west1", label: "us-west1" },
    { value: "europe-west4", label: "europe-west4" },
    { value: "europe-west1", label: "europe-west1" },
    { value: "asia-south2", label: "asia-south2" },
    { value: "asia-south1", label: "asia-south1" },
  ],
  None: [
    { value: "us-central1", label: "us-central1" },
    { value: "us-east5", label: "us-east5" },
    { value: "us-east1", label: "us-east1" },
    { value: "us-east4", label: "us-east4" },
    { value: "us-south1", label: "us-south1" },
    { value: "us-west3", label: "us-west3" },
    { value: "us-west1", label: "us-west1" },
    { value: "northamerica-northeast2", label: "northamerica-northeast2" },
    { value: "northamerica-south1", label: "northamerica-south1" },
    { value: "europe-west1", label: "europe-west1" },
    { value: "europe-west2", label: "europe-west2" },
    { value: "europe-west3", label: "europe-west3" },
    { value: "europe-west4", label: "europe-west4" },
    { value: "europe-west9", label: "europe-west9" },
    { value: "europe-southwest1", label: "europe-southwest1" },
    { value: "europe-north2", label: "europe-north2" },
    { value: "asia-northeast1", label: "asia-northeast1" },
    { value: "asia-northeast3", label: "asia-northeast3" },
    { value: "asia-south1", label: "asia-south1" },
    { value: "asia-southeast1", label: "asia-southeast1" },
    { value: "australia-southeast1", label: "australia-southeast1" },
  ],
};

function buildVariantInfo() {
  const info: Record<string, number[]> = {};
  Object.keys(GPU_CPU_MAP).forEach((display) => {
    const [countWithX, model, vramWithG] = display.split(" ");
    const count = parseInt(countWithX.slice(0, -1), 10);
    const variant = `${model} ${vramWithG}`; // e.g. "A100 40G"
    if (!info[variant]) info[variant] = [];
    if (!info[variant].includes(count)) info[variant].push(count);
  });
  Object.values(info).forEach((arr) => arr.sort((a, b) => a - b));
  return info as Record<GpuVariant, number[]>;
}

const VARIANT_INFO = buildVariantInfo();
const GPU_VARIANTS: GpuVariant[] = ["None", ...Object.keys(VARIANT_INFO).filter((k) => k !== "None")] as GpuVariant[];

function machineTypeToGpuState(machineType: string): { variant: GpuVariant; count: number } {
  const entry = Object.entries(GPU_CPU_MAP).find(([, v]) => v.value === machineType);
  if (!entry) return { variant: "None", count: 1 };

  const displayKey = entry[0]; // "4x A100 40G"
  const [countWithX, ...variantParts] = displayKey.split(" ");
  const count = parseInt(countWithX.slice(0, -1), 10);
  const variant = variantParts.join(" ") as GpuVariant;
  return { variant, count };
}

export const SettingsForm = forwardRef<{ isRegionValid: () => boolean }, SettingsFormProps>(
  ({ isEditing, onChange }, ref) => {
    const { settings, setSettings } = useSettings();

    const users = settings.users ?? [];
    const [newUser, setNewUser] = useState("");

    const initialGpu = machineTypeToGpuState(settings.machineType);
    const [gpuVariant, setGpuVariant] = useState<GpuVariant>(initialGpu.variant);
    const [gpusPerVm, setGpusPerVm] = useState<number>(initialGpu.count);
    const [cpuChoice, setCpuChoice] = useState<string>(
      initialGpu.variant === "None" ? settings.machineType : CPU_OPTIONS[1].value
    );

    // Keep local form state synced to settings.machineType
    useEffect(() => {
      const nextGpu = machineTypeToGpuState(settings.machineType);
      if (nextGpu.variant !== gpuVariant) setGpuVariant(nextGpu.variant);
      if (nextGpu.count !== gpusPerVm) setGpusPerVm(nextGpu.count);

      if (nextGpu.variant === "None" && settings.machineType !== cpuChoice) {
        setCpuChoice(settings.machineType);
      }
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [settings.machineType]);

    // Push machineType back into settings when local choices change
    useEffect(() => {
      const desiredMachineType =
        gpuVariant === "None"
          ? cpuChoice
          : GPU_CPU_MAP[`${gpusPerVm}x ${gpuVariant}`]?.value ?? settings.machineType;

      if (settings.machineType !== desiredMachineType) {
        setSettings((prev) => ({ ...prev, machineType: desiredMachineType }));
        onChange?.();
      }
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gpuVariant, gpusPerVm, cpuChoice]);

    const labelClass = "block text-sm font-medium text-gray-500 mb-1";

    const regionOptions = useMemo(() => REGION_OPTIONS[gpuVariant], [gpuVariant]);
    const isRegionValid = regionOptions.some((r) => r.value === settings.gcpRegion);

    useImperativeHandle(ref, () => ({ isRegionValid: () => isRegionValid }), [isRegionValid]);

    useEffect(() => {
      if (!isEditing && !isRegionValid) {
        toast({ title: "Please select a region from dropdown", variant: "destructive" });
      }
    }, [isEditing, isRegionValid]);

    const handleInputChange = <K extends keyof SettingsData>(key: K, value: SettingsData[K]) => {
      setSettings((prev) => {
        const changed = prev[key] !== value;
        if (!changed) return prev;
        onChange?.();
        return { ...prev, [key]: value };
      });
    };

    const addUser = () => {
      const email = newUser.trim();
      if (!email) return;

      const exists = users.some((u) => u.toLowerCase() === email.toLowerCase());
      if (exists) {
        setNewUser("");
        return;
      }

      setSettings((prev) => ({ ...prev, users: [...users, email] }));
      setNewUser("");
      onChange?.();
    };

    const removeUser = (user: string) => {
      setSettings((prev) => ({ ...prev, users: users.filter((u) => u !== user) }));
      onChange?.();
    };

    return (
      <div className="space-y-12 overflow-hidden max-w-6xl mx-auto w-full">
        <Card className="w-full">
          <CardContent className="space-y-12 pt-6">
            <div className="space-y-2">
              <h2 className="text-xl font-semibold text-primary">Container Image</h2>
              <div className="grid grid-cols-1 gap-4">
                <div>
                  <div className="flex items-center gap-1">
                    <label className={labelClass}>Image URI</label>
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <InfoIcon className="h-4 w-4 text-gray-400 hover:text-gray-600 cursor-help -mt-2" />
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>
                            URI of the Docker image to run your code inside.
                            <br />
                            This can be any image, as long as it has Python installed.
                            <br />
                            Private images are pulled using the host VM's service account credentials.
                          </p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </div>
                  <Input
                    disabled={!isEditing}
                    className="w-full h-9.5"
                    value={settings.containerImage}
                    onChange={(e) => handleInputChange("containerImage", e.target.value)}
                  />
                </div>
              </div>
            </div>

            <div className="space-y-4">
              <h2 className="text-xl font-semibold text-primary">Virtual Machines</h2>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="flex flex-col space-y-2">
                  <label className={labelClass}>Quantity</label>
                  <Input
                    type="number"
                    disabled={!isEditing}
                    className="h-9.5 w-full"
                    min={1}
                    max={1000}
                    value={settings.machineQuantity || ""}
                    onChange={(e) => {
                      const raw = e.target.value;
                      const num = parseInt(raw, 10);
                      if (!isNaN(num)) handleInputChange("machineQuantity", num);
                      else if (raw === "") handleInputChange("machineQuantity", 0);
                    }}
                    onBlur={(e) => {
                      const val = parseInt(e.target.value, 10);
                      if (val < 1) handleInputChange("machineQuantity", 1);
                      else if (val > 1000) handleInputChange("machineQuantity", 1000);
                    }}
                  />
                </div>

                <div className="flex flex-col space-y-2">
                  <label className={labelClass}>vCPU / RAM</label>
                  <Select
                    disabled={!isEditing || gpuVariant !== "None"}
                    value={
                      gpuVariant === "None"
                        ? cpuChoice
                        : GPU_CPU_MAP[`${gpusPerVm}x ${gpuVariant}`].value
                    }
                    onValueChange={(val) => {
                      setCpuChoice(val);
                      setSettings((prev) => ({ ...prev, machineType: val }));
                      onChange?.();
                    }}
                  >
                    <SelectTrigger className="w-full h-9.5">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {(gpuVariant === "None"
                        ? CPU_OPTIONS
                        : [GPU_CPU_MAP[`${gpusPerVm}x ${gpuVariant}`]]
                      ).map((o) => (
                        <SelectItem key={o.value} value={o.value}>
                          {o.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex flex-col space-y-2">
                  <label className={labelClass}>GPU</label>
                  <Select
                    disabled={!isEditing}
                    value={gpuVariant}
                    onValueChange={(val) => {
                      setGpuVariant(val as GpuVariant);
                      if (val === "None") setGpusPerVm(1);
                      else setGpusPerVm(VARIANT_INFO[val as GpuVariant][0]);
                    }}
                  >
                    <SelectTrigger className="w-full h-9.5">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {GPU_VARIANTS.map((model) => (
                        <SelectItem key={model} value={model}>
                          {model}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex flex-col space-y-2">
                  <label className={labelClass}>GPUs per VM</label>

                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <div className={gpuVariant === "None" && isEditing ? "opacity-50 cursor-default" : ""}>
                          <Select
                            disabled={!isEditing || gpuVariant === "None"}
                            value={gpusPerVm.toString()}
                            onValueChange={(val) => setGpusPerVm(parseInt(val, 10))}
                          >
                            <SelectTrigger className="w-full h-9.5 disabled:cursor-default disabled:opacity-100">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {(gpuVariant === "None" ? [1] : VARIANT_INFO[gpuVariant]).map((n) => (
                                <SelectItem key={n} value={n.toString()}>
                                  {n}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                      </TooltipTrigger>

                      {gpuVariant === "None" && isEditing && (
                        <TooltipContent>
                          <p>Select a GPU to edit</p>
                        </TooltipContent>
                      )}
                    </Tooltip>
                  </TooltipProvider>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
                <div className="flex flex-col space-y-2">
                  <label className={labelClass}>Disk Size (GB)</label>
                  <Input
                    type="number"
                    disabled={!isEditing}
                    className="w-full h-9.5"
                    min={10}
                    max={2000}
                    value={settings.diskSize || ""}
                    onChange={(e) => {
                      const raw = e.target.value;
                      const num = parseInt(raw, 10);
                      if (!isNaN(num)) handleInputChange("diskSize", num);
                      else if (raw === "") handleInputChange("diskSize", 0);
                    }}
                    onBlur={(e) => {
                      const val = parseInt(e.target.value, 10);
                      if (val < 10) handleInputChange("diskSize", 10);
                      else if (val > 2000) handleInputChange("diskSize", 2000);
                    }}
                  />
                </div>

                <div className="flex flex-col space-y-2">
                  <label className={labelClass}>GCP Region</label>
                  <Select
                    disabled={!isEditing}
                    value={settings.gcpRegion || ""}
                    onValueChange={(val) => handleInputChange("gcpRegion", val)}
                  >
                    <SelectTrigger
                      className={`w-full h-9.5 ${
                        !isRegionValid && isEditing ? "border-red-500 focus:ring-red-500 ring-2" : ""
                      }`}
                    >
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {regionOptions.map((region) => (
                        <SelectItem key={region.value} value={region.value}>
                          {region.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {!isRegionValid && isEditing && (
                    <span className="text-xs text-red-600 mt-1">Please select a region from dropdown</span>
                  )}
                </div>

                <div className="flex flex-col space-y-2">
                  <label className={labelClass}>Inactivity Timeout (minutes)</label>
                  <Input
                    type="number"
                    disabled={!isEditing}
                    className="w-full h-9.5"
                    min={1}
                    max={1440}
                    value={settings.inactivityTimeout || ""}
                    onChange={(e) => {
                      const raw = e.target.value;
                      const num = parseInt(raw, 10);
                      if (!isNaN(num)) handleInputChange("inactivityTimeout", num);
                      else if (raw === "") handleInputChange("inactivityTimeout", 0);
                    }}
                    onBlur={(e) => {
                      const val = parseInt(e.target.value, 10);
                      if (val < 1) handleInputChange("inactivityTimeout", 1);
                      else if (val > 1440) handleInputChange("inactivityTimeout", 1440);
                    }}
                  />
                </div>
              </div>
            </div>

            <div className="space-y-2">
              <h2 className="text-xl font-semibold text-primary">Authorized Users</h2>
              <div>
                <div className="flex items-center gap-1">
                  <label className={labelClass}>Add User Email</label>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <InfoIcon className="h-4 w-4 text-gray-400 hover:text-gray-600 cursor-help -mt-2" />
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>Google accounts authorized to use this deployment.</p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                </div>

                <form
                  onSubmit={(e) => {
                    e.preventDefault();
                    if (isEditing) addUser();
                  }}
                  className="flex gap-2 w-full"
                >
                  <Input
                    disabled={!isEditing}
                    className="w-full h-9.5"
                    value={newUser}
                    onChange={(e) => setNewUser(e.target.value)}
                  />
                  <Button
                    type="button"
                    onClick={() => isEditing && addUser()}
                    disabled={!isEditing}
                    variant="secondary"
                  >
                    Add
                  </Button>
                </form>
              </div>

              <div className="flex flex-wrap gap-2">
                {users.map((user) => (
                  <span
                    key={user}
                    className="bg-gray-100 border border-gray-300 text-gray-800 px-2 py-1 rounded-md flex items-center gap-1"
                  >
                    {user}
                    {isEditing && (
                      <button
                        onClick={() => removeUser(user)}
                        className="text-gray-500 hover:text-gray-700 text-xl leading-none"
                      >
                        ×
                      </button>
                    )}
                  </span>
                ))}
              </div>
            </div>
          </CardContent>

          {(settings.burlaVersion || settings.googleCloudProjectId) && (
            <div
              className="flex items-center justify-between rounded-b-lg"
              style={{
                background: "#f8f9fa",
                borderTop: "1px solid #e5e7eb",
                fontSize: "0.8em",
                color: "#8c939f",
                padding: "10px 24px",
                minHeight: 40,
              }}
            >
              <span style={{ fontWeight: 400 }}>
                {settings.burlaVersion && <>Version: {settings.burlaVersion}</>}
              </span>
              <span style={{ fontWeight: 400, textAlign: "right" }}>
                {settings.googleCloudProjectId && <>Google Cloud Project: {settings.googleCloudProjectId}</>}
              </span>
            </div>
          )}
        </Card>
      </div>
    );
  }
);

SettingsForm.displayName = "SettingsForm";
