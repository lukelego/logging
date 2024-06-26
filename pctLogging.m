classdef pctLogging
    methods (Static)
        function setParallelLogging(cluster, level, opts)
            % pctLogging.setParallelLogging(CLUSTER, 'on')
            % enables additional logging on the cluster CLUSTER
            %
            % pctLogging.setParallelLogging(CLUSTER, 'medium')
            % enables additional logging at a medium level on the cluster CLUSTER
            %
            % pctLogging.setParallelLogging(CLUSTER, 'custom', clusterLogLevel = N, clientLogLevel = M)
            % enables additional logging on the cluster CLUSTER and sets the
            % cluster log level and client log level.
            %
            % pctLogging.setParallelLogging(CLUSTER, 'off')
            % disables additional logging on the cluster CLUSTER
            %
            %
            % CLUSTER - cluster specified as a cluster object or the name of a valid Cluster Profile.
            %
            % Level - level of additional logging on the cluster and the client, specified
            % as one of these options: "on", "off" , "low", "medium", "high", or "custom".
            % When specifying "custom" both clusterLogLevel and clientLogLevel must be specified.
            %
            % clusterLogLevel - level of additional logging on the cluster, specified
            % as one of these options: "low", "medium", "high" or an integer between 0 and 6.
            %
            % clientLogLevel - level of additional logging on the client, specified
            % as one of these options: "low", "medium", "high" or an integer between 0 and 6.
            %
            % Examples:
            %
            % % Enable additional logging for a cluster named 'MYCLUSTER'
            % pctLogging.setParallelLogging('MYCLUSTER', 'on')
            %
            % % Enable additional logging for a cluster identified by the cluster object
            % % c and set the cluster log level to "medium" and the client log level to 3
            % pctLogging.setParallelLogging(c, 'custom', clusterLogLevel="medium", clientLogLevel=3)
            %
            % %Disable additional logging for an cluster named 'MYCLUSTER'
            % pctLogging.setParallelLogging('MYCLUSTER', 'off')

            % Copyright 2023 The MathWorks, Inc.

            arguments
                cluster % either a profile name or a cluster object
                level {pctLogging.mustBeValidLogLevel(level)};
                opts.clusterLogLevel {pctLogging.mustBeValidCustomLogLevel(level, opts.clusterLogLevel)}; % low 2, medium 4, high 5
                opts.clientLogLevel  {pctLogging.mustBeValidCustomLogLevel(level, opts.clientLogLevel)}; % low 2, medium 4, high 6
            end
            if string(level) == "custom"
                if ~isfield(opts, "clusterLogLevel") || ~isfield(opts, "clientLogLevel")
                    throwAsCaller(MException("Parallel:Logging:MissingCustomLogLevel", "When using log level ""custom"", both clusterLogLevel and clientLogLevel must be specified"))
                end
            else
                opts.clusterLogLevel = level;
                opts.clientLogLevel = level;
            end

            c = pctLogging.getClusterObject(cluster);

            if string(level) == "off"
                iCleanupLogging(c);
            else
                iSetupLogging(c, opts.clusterLogLevel, opts.clientLogLevel);
            end

            function iSetupLogging(cluster, clusterLogLevel, clientLogLevel)
                switch cluster.Type
                    case "Local"
                        iSetupLoggingProcesses(clusterLogLevel);
                    case {"MJS", "MJSComputeCloud"} % allows for cloud center clusters
                        iSetupLoggingMJS(cluster, clusterLogLevel);
                    case {"HPCServer", "Generic"}
                        iSetupLoggingCJSCommon(clusterLogLevel);
                    otherwise
                        throwAsCaller(MException("Parallel:Logging:InvalidClusterType", "Provided cluster type is not valid for this function"));
                end
                iSetupLoggingClient(clientLogLevel);
            end

            function iCleanupLogging(cluster)
                switch cluster.Type
                    case "Local"
                        iCleanupLoggingCJSCommon();
                        % this switches off the C++ logging
                        feature("diagnosticDest", "");
                    case {"MJS", "MJSComputeCloud"}
                        iCleanupLoggingMJS(cluster);
                    case {"HPCServer", "Generic"}
                        iCleanupLoggingCJSCommon();
                    otherwise
                        throwAsCaller(MException("Parallel:Logging:InvalidClusterType", "Provided cluster type is not valid for this function"));
                end
                parallel.internal.logging.enableClientLogging('');
            end
            %% Client Logging
            function iSetupLoggingClient(clientLogLevel)

                % making a directory
                clientLogDir = fullfile(prefdir(), "ParallelLogs", "clientLogs");
                pctLogging.createLogDir(clientLogDir);

                if matlab.internal.datatypes.isScalarText(clientLogLevel)
                    if clientLogLevel == "high" || clientLogLevel == "on"
                        newLevel = 5;
                    elseif clientLogLevel == "medium"
                        newLevel = 4;
                    elseif clientLogLevel == "low"
                        newLevel = 2;
                    end
                else
                    newLevel = clientLogLevel;
                end
               parallel.internal.logging.enableClientLogging(clientLogDir, newLevel);
            end

            %%Common to both Processes and 3rd Party Generic
            function iSetupLoggingCJSCommon(clusterLogLevel)
                % used for all third party schedulers, we'll set all the ways of enabling
                % logs to be sure
                if matlab.internal.datatypes.isScalarText(clusterLogLevel)
                    if clusterLogLevel == "high" || clusterLogLevel == "on"
                        newLevel = 5;
                    elseif clusterLogLevel == "medium"
                        newLevel = 4;
                    elseif clusterLogLevel == "low"
                        newLevel = 2;
                    end
                else
                    newLevel = clusterLogLevel;
                end
                setenv("MDCE_DEBUG", string(newLevel)); %%parallel server debug
                pctconfig("preservejobs", true);
            end

            function iCleanupLoggingCJSCommon()
                setenv("MDCE_DEBUG", "false");
                pctconfig("preservejobs", false);
            end

            %%Local/Processes Scheduler
            function iSetupLoggingProcesses(clusterLogLevel)
                iSetupLoggingCJSCommon(clusterLogLevel);
                cppLogDir = fullfile(prefdir(), "ParallelLogs", "cppLogs");
                pctLogging.createLogDir(cppLogDir);

                feature("diagnosticSpec", "parallel::localscheduler.*=all");
                feature("diagnosticDest", sprintf("file='%s%s%s'", cppLogDir, filesep, "mwlog.txt"));
            end

            function iSetupLoggingMJS(c, clusterLogLevel)
                if matlab.internal.datatypes.isScalarText(clusterLogLevel)
                    if clusterLogLevel == "high" || clusterLogLevel == "on"
                        newLevel = 5;
                    elseif clusterLogLevel == "medium"
                        newLevel = 4;
                    elseif clusterLogLevel == "low"
                        newLevel = 2;
                    end
                else
                    newLevel = clusterLogLevel;
                end

                fprintf("Current cluster log level is %i, setting to %i. You can return to your original cluster log level after testing is finished by running \n\nc = parcluster(""%s"")\nc.ClusterLogLevel = %i;\n", c.ClusterLogLevel, newLevel, c.Profile, c.ClusterLogLevel)
                c.ClusterLogLevel = newLevel;
            end

            function iCleanupLoggingMJS(c)
                c.ClusterLogLevel = 0;
            end
        end
        %% Function to validate log level arguments
        function mustBeValidLogLevel(logValue)
            if  matlab.internal.datatypes.isScalarText(logValue)
                if ismember(logValue, ["low", "medium", "high", "on", "off", "custom"])
                    return;
                end
            elseif isnumeric(logValue) && isscalar(logValue)
                if ismember(logValue, [0 1 2 3 4 5 6])
                    return;
                end
            end
            throwAsCaller(MException("Parallel:Logging:InvalidLogLevel", "Log level must be ""low"", ""medium"", ""high"", or an integer from 0-6"))
        end

        function mustBeValidCustomLogLevel(level, logValue)
            if level == "custom"
                if  matlab.internal.datatypes.isScalarText(logValue)
                    if ismember(logValue, ["low", "medium", "high"])
                        return;
                    end
                elseif isnumeric(logValue) && isscalar(logValue)
                    if ismember(logValue, [0 1 2 3 4 5 6])
                        return;
                    end
                end
            else
                throwAsCaller(MException("Parallel:Logging:InvalidLevelCombination", "Argument may only be used with a level of ""custom"""))
            end
            throwAsCaller(MException("Parallel:Logging:InvalidLogLevel", "Log level must be ""low"", ""medium"", ""high"", or an integer from 0-6"))
        end

        function gatherParallelLogs(cluster, opts)
            % pctLogging.gatherParallelLogs(CLUSTER)
            % gathers log files for the cluster CLUSTER and saves them as a
            % zip file in the current folder.
            %
            % pctLogging.gatherParallelLogs(CLUSTER, saveLocation = SAVELOCATION)
            % gathers all log files for the cluster CLUSTER and saves them as a
            % zip file in the location specified by SAVELOCATION.
            %
            % pctLogging.gatherParallelLogs(CLUSTER, additionalFiles= {FILE1, FILE2, ...})
            % gathers all log files for the cluster CLUSTER and all files or
            % folders specified by additionalFiles and saves them as a zip file in
            % the current folder.
            %
            %Examples:
            %
            % %Gather all log files for a cluster named "MYCLUSTER"
            % pctLogging.gatherParallelLogs("MYCLUSTER")
            %
            % %Gather all log files for a cluster identified by the cluster
            % object C and save them in the E:\MyFolder folder
            % pctLogging.gatherParallelLogs(c, saveLocation = "E:\myFolder")
            %
            % %Gather all  files for the cluster identified by the cluster
            % object C and the file E:\myFile and save them in the E:\MyFolder folder
            % pctLogging.gatherParallelLogs(c, saveLocation = "E:\myFolder", additionalFiles= "E:\myFile")

            % Copyright 2023 The MathWorks, Inc.
            arguments
                cluster % error handling managed by getClusterObject function
                opts.saveLocation {mustBeFolder(opts.saveLocation)} = "."
                opts.additionalFiles {mustBeText(opts.additionalFiles)} = string.empty;
            end

            opts.saveLocation = string(opts.saveLocation);
            opts.additionalFiles = string(opts.additionalFiles);

            [~, values] = fileattrib(opts.saveLocation);
            if ~values.UserWrite
                throwAsCaller(MException("Parallel:Logging:SaveLocationIsReadOnly", "Specified saveLocation is not writable"))
            end

            c = pctLogging.getClusterObject(cluster);
            if ~isempty(c.Profile)
                logFolderName = sprintf("%s-logs-%s", c.Profile, string(datetime, "yyyyMMdd-HHmmSS"));
            else
                logFolderName = sprintf("%sCluster-logs-%s", c.Type, string(datetime, "yyyyMMdd-HHmmSS"));
            end

            tmpLogFolder = fullfile(tempdir(), logFolderName);
            pctLogging.createLogDir(tmpLogFolder);
            cleanup = onCleanup(@()rmdir(tmpLogFolder, "s"));

            switch c.Type
                case "Local"
                    iGatherLoggingProcesses(c, tmpLogFolder);
                case {"MJS", "MJSComputeCloud"}
                    iGatherLoggingMJS(c, tmpLogFolder)
                case {"HPCServer", "Generic"}
                    iGatherLoggingCJS(c, tmpLogFolder);
                otherwise
                    throwAsCaller(MException("Parallel:Logging:InvalidClusterType", "Provided cluster type is not valid for this function"));
            end

            iGatherLoggingClient(tmpLogFolder);

            if numel(opts.additionalFiles) > 0
                iGatherAdditionalFiles(tmpLogFolder, opts.additionalFiles);
            end

            % everything should now be in the log folder, let's zip that all together
            zipFileName = sprintf("%s.zip", logFolderName);
            zipFileLocation = fullfile(tempdir(), zipFileName);
            zip(zipFileLocation, tmpLogFolder);
            movefile(zipFileLocation, opts.saveLocation);

            function iGatherAdditionalFiles(logFolder, additionalFileList)
                additionalFilesFolder = fullfile(logFolder, "additionalFiles");

                pctLogging.createLogDir(additionalFilesFolder);

                for ii = 1:numel(additionalFileList)
                    if isfolder(additionalFileList(ii))
                        fileList = iFindAdditionalFiles(additionalFileList(ii));
                        parentDir = parallel.internal.apishared.FilenameUtils.getParentDirectory(additionalFileList(ii));
                        iProcessAndCopyFileList(fileList, additionalFilesFolder, parentDir);
                    else
                        iProcessAndCopyFileList(additionalFileList(ii), additionalFilesFolder)
                    end
                end
            end

            function fileList = iFindAdditionalFiles(folderPath)
                % convert directory into list of files
                fileList = parallel.internal.apishared.FilenameUtils.listAll(folderPath);
                fileList = string(fileList);
            end

            function iGatherLoggingClient(logFolder)
                fileList = iFindClientLogFiles();
                if fileList ~= ""
                    iProcessAndCopyFileList(fileList, logFolder, fullfile(prefdir(), "ParallelLogs"))
                end
            end

            function iGatherLoggingCJS(c, saveLocation)
                clusterJSL = pctLogging.getJobStorageLocation(c);
                fileList = iFindJobLogs(clusterJSL);
                iProcessAndCopyFileList(fileList, saveLocation, clusterJSL)
            end

            function iGatherLoggingProcesses(c, saveLocation)
                clusterJSL = pctLogging.getJobStorageLocation(c);
                fileList = iFindJobLogs(clusterJSL);
                iProcessAndCopyFileList(fileList, saveLocation, clusterJSL);
                fileList = iFindLocalSchedCppLogs();
                iProcessAndCopyFileList(fileList, saveLocation, fullfile(prefdir(), "ParallelLogs"));
            end

            function iGatherLoggingMJS(c, saveLocation)
                fprintf("Gathering cluster log files from a MJS cluster can take several minutes if the number of log files is large.\n");
                getClusterLogs(c, saveLocation);
            end

            function logFolderName = iFindClientLogFiles()
                clientLogDir = fullfile(prefdir(), "ParallelLogs", "clientLogs");

                if isfolder(clientLogDir)
                    logFolderName = string(clientLogDir);
                else
                    logFolderName = "";
                end
            end

            function fileList = iFindJobLogs(logDir)
                % grab everything that isn't a .mat file
                % parse through the job storage location looking for file endings
                % associated with job/task logs
                fileList = parallel.internal.apishared.FilenameUtils.listAll(logDir);
                fileList = string(fileList);
                fileList = fileList(~contains(fileList, ".mat"));
                fileList = fileList(~contains(fileList, ".lck"));
                fileList = fileList(~contains(fileList, "metadata"));
                fileList = fileList(~contains(fileList, "matlab_mirror"));
            end

            function fileList = iFindLocalSchedCppLogs()
                % go find the local scheduler's C++ logs
                cppLogDir = fullfile(prefdir(), "ParallelLogs", "cppLogs");
                fileList = parallel.internal.apishared.FilenameUtils.listAll(cppLogDir);
                fileList = string(fileList);
            end

            function iProcessAndCopyFileList(fileList, saveLocation, baseDir)
                for ii = 1:numel(fileList)
                    if exist(fileList(ii), "file") || exist(fileList(ii), "dir")
                        % to avoid naming clashes between logs from multiple jobs we're
                        % going to use the parent directory as well in the folder we're
                        % gathering to
                        if(nargin>2)
                            relativePath = parallel.internal.apishared.FilenameUtils.getRelativePath(baseDir, fileList(ii));
                            if(ispc()) && contains(relativePath, ":") %relative path that contains drive letter
                                relativePath = extractAfter(relativePath, ":");
                            end
                            [filePath, ~, ~] = fileparts(relativePath);
                            pctLogging.createLogDir(fullfile(saveLocation, filePath));
                            copyfile(fileList(ii), fullfile(saveLocation, relativePath));
                        else
                            copyfile(fileList(ii), fullfile(saveLocation));
                        end
                    end
                end
            end
        end
        function cluster = getClusterObject(clusterInput)
            % getClusterObject(clusterInput) returns a cluster object for either a cluster
            % object or cluster profile name.

            % Copyright 2023 The MathWorks, Inc.
            if matlab.internal.datatypes.isScalarText(clusterInput)
                try
                    cluster = parcluster(clusterInput);
                catch E
                    if strcmp(E.identifier,'parallel:settings:DisallowedThreadsProfile')
                        error("Logging is not supported for the ""Threads"" profile. Use an alternative profile.")
                    else
                        throw(E)
                    end
                end
            elseif isa(clusterInput, 'parallel.Cluster')
                cluster = clusterInput;
            else
                error('Argument is not a valid profile name or cluster object')
            end
        end

        function clusterJSL = getJobStorageLocation(cluster)
            %getJobStorageLocation(cluster) returns the job storage location of a
            %cluster profile for the current operating system.

            % Copyright 2011-2023 The MathWorks, Inc.

            clusterJSL=cluster.JobStorageLocation;
            if isstruct(clusterJSL)
                if ispc
                    clusterJSL=clusterJSL.windows;
                else
                    clusterJSL=clusterJSL.unix;
                end
            end
        end

        function createLogDir(dirName)
            % createLogDir(dirName) creates a directory named dirName if it does not exist

            % Copyright 2023 The MathWorks, Inc.

            if ~exist( dirName, 'dir' )
                [status, msg, messageid] = mkdir(dirName);
                if ~status || ~exist( dirName, 'dir' )
                    mkdirError = MException( messageid, '%s', msg );
                    err = MException( message( 'parallel:cluster:CouldNotCreateTempDir' ) );
                    err = err.addCause( mkdirError );
                    throw( err );
                end
            end
        end
    end
end