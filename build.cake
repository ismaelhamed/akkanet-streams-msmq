var target = Argument("Target", "Default");
var configuration = Argument("Configuration", "Release");
var releaseNote = ParseReleaseNotes("./RELEASE_NOTES.md");
var nugetPrefix = "";

Task("Clean")
    .Description("Cleans the Artifacts, bin and obj directories.")
    .Does(() =>
    {
        CleanDirectory("./artifacts");
        DeleteDirectories(GetDirectories("**/bin"), new DeleteDirectorySettings() { Force = true, Recursive = true });
        DeleteDirectories(GetDirectories("**/obj"), new DeleteDirectorySettings() { Force = true, Recursive = true });
    });

Task("Restore")
    .Description("Restores NuGet packages.")
    .IsDependentOn("Clean")
    .Does(() =>
    {
        DotNetRestore("./src", new DotNetRestoreSettings
        {
            Verbosity = DotNetVerbosity.Minimal,
            Sources = new []
            {
                "https://api.nuget.org/v3/index.json"
            }
        });
    });

Task("Build")
    .Description("Builds the solution.")
    .IsDependentOn("Restore")
    .Does(() =>
    {
        DotNetBuild("./src", new DotNetBuildSettings()
        {
            Configuration = configuration,
            NoRestore = true,
            MSBuildSettings = new DotNetMSBuildSettings()
                .SetVersion(releaseNote.SemVersion.ToString())
                .SetAssemblyVersion(releaseNote.SemVersion.ToString())
                .SetFileVersion(releaseNote.SemVersion.ToString())
        });
    });

Task("Test")
    .Description("Runs unit tests and outputs test results to the Artifacts directory.")
    .DoesForEach(GetFiles("./src/**/*.Tests.csproj"), project =>
    {
        DotNetTest(project.ToString(), new DotNetTestSettings()
        {
            Blame = true,
            Configuration = configuration,
            Loggers = new string[]
            {
                $"trx;LogFileName={project.GetFilenameWithoutExtension()}.trx",
                $"html;LogFileName={project.GetFilenameWithoutExtension()}.html",
            },
            NoBuild = true,
            NoRestore = true,
            ResultsDirectory = "./artifacts"
        });
    });

Task("Pack")
    .Description("Creates NuGet packages and outputs them to the artifacts directory.")
    .IsDependentOn("Test")
    .DoesForEach(GetFiles("./src/**/*.csproj"), project =>
    {
        DotNetPack(project.ToString(), new DotNetPackSettings()
        {
            Configuration = configuration,
            IncludeSymbols = true,
            NoBuild = true,
            NoRestore = true,
            OutputDirectory = "./artifacts/nuget",
            MSBuildSettings = new DotNetMSBuildSettings()
                .SetContinuousIntegrationBuild(!BuildSystem.IsLocalBuild)
                .SetPackageVersion(releaseNote.SemVersion.ToString())
                .SetPackageReleaseNotes(string.Join(Environment.NewLine, releaseNote.Notes))
        });
    });

Task("Default")
    .Description("Cleans, restores NuGet packages, builds the solution, runs unit tests, and then publishes packages.")
    .IsDependentOn("Build")
    .IsDependentOn("Test")
    .IsDependentOn("Pack");

RunTarget(target);